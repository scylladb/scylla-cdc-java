package com.scylladb.cdc.lib;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.master.GenerationMetadata;
import com.scylladb.cdc.model.worker.TaskState;
import com.scylladb.cdc.model.worker.Worker;
import com.scylladb.cdc.model.worker.WorkerConfiguration;
import com.scylladb.cdc.transport.GroupedTasks;
import com.scylladb.cdc.transport.MasterTransport;
import com.scylladb.cdc.transport.TaskAbortedException;
import com.scylladb.cdc.transport.WorkerTransport;

class LocalTransport implements MasterTransport, WorkerTransport {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final ThreadGroup workersThreadGroup;
    private final WorkerConfiguration.Builder workerConfigurationBuilder;
    private final Supplier<ScheduledExecutorService> executorServiceSupplier;
    private final CDCStateStore stateStore;

    /**
     * Tracks which task IDs are currently active (have been set via {@link #setState}).
     * Used by {@link #updateState} and {@link #moveStateToNextWindow} to detect
     * {@link TaskAbortedException} without relying on a store read, which would be
     * incompatible with eventually-consistent backends.
     */
    private final Set<TaskId> activeTasks = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private Optional<GenerationId> currentGenerationId = Optional.empty();

    // Single worker reference
    private Worker currentWorker = null;
    private Thread workerThread = null;

    // Track generation IDs by table for tablet mode
    protected final Map<TableName, GenerationMetadata> currentGenerationByTable = new ConcurrentHashMap<>();

    public LocalTransport(ThreadGroup cdcThreadGroup, WorkerConfiguration.Builder workerConfigurationBuilder,
                          Supplier<ScheduledExecutorService> executorServiceSupplier,
                          CDCStateStore stateStore) {
        workersThreadGroup = new ThreadGroup(cdcThreadGroup, "Scylla-CDC-Worker-Threads");
        this.workerConfigurationBuilder = Preconditions.checkNotNull(workerConfigurationBuilder);
        this.executorServiceSupplier = Preconditions.checkNotNull(executorServiceSupplier);
        this.stateStore = Preconditions.checkNotNull(stateStore);
    }

    @Override
    public Optional<GenerationId> getCurrentGenerationId() {
        return currentGenerationId;
    }

    @Override
    public Optional<GenerationId> getCurrentGenerationId(TableName tableName) {
        GenerationMetadata metadata = currentGenerationByTable.get(tableName);
        if (metadata == null) {
            return Optional.empty();
        }
        return Optional.of(metadata.getId());
    }

    @Override
    public boolean areTasksFullyConsumedUntil(Set<TaskId> tasks, Timestamp until) {
        return stateStore.areTasksFullyConsumedUntil(tasks, until);
    }

    @Override
    public void configureWorkers(GroupedTasks workerTasks) throws InterruptedException {
        Map<TaskId, SortedSet<StreamId>> tasks = workerTasks.getTasks();

        // Determine which tasks are being removed and clean them up
        Set<TaskId> toDelete = new HashSet<>(activeTasks);
        toDelete.removeAll(tasks.keySet());
        if (!toDelete.isEmpty()) {
            activeTasks.removeAll(toDelete);
            stateStore.deleteTaskStates(toDelete);
        }

        currentGenerationId = Optional.ofNullable(workerTasks.getGenerationId());
        if (workerTasks.getGenerationId() != null) {
            stateStore.saveGenerationId(workerTasks.getGenerationId());
        }

        // Stop current worker if exists
        stopWorkerThread();

        // Create and start a new worker
        startNewWorkerThread(workerTasks);
    }

    @Override
    public void configureWorkers(TableName tableName, GroupedTasks workerTasks) throws InterruptedException {
        Map<TaskId, SortedSet<StreamId>> tasks = workerTasks.getTasks();

        // Determine which tasks for this table are being removed
        Set<TaskId> toDelete = new HashSet<>();
        for (TaskId taskId : activeTasks) {
            if (taskId.getTable().equals(tableName) && !tasks.containsKey(taskId)) {
                toDelete.add(taskId);
            }
        }
        if (!toDelete.isEmpty()) {
            activeTasks.removeAll(toDelete);
            stateStore.deleteTaskStates(toDelete);
        }

        // Update generation metadata for this table
        currentGenerationByTable.put(tableName, workerTasks.getGenerationMetadata());
        stateStore.saveGenerationId(tableName, workerTasks.getGenerationMetadata().getId());

        if (currentWorker == null) {
            // No worker exists, start a new one
            startNewWorkerThread(workerTasks);
        } else {
            if (!tasks.isEmpty()) {
                try {
                    currentWorker.addTasks(workerTasks);
                } catch (ExecutionException e) {
                    logger.atSevere().withCause(e).log("Error adding tasks for table %s", tableName);
                    throw new RuntimeException("Error adding tasks", e);
                }
            }
        }
    }

    @Override
    public void stopWorkers() throws InterruptedException {
        stopWorkerThread();
    }

    private void startNewWorkerThread(GroupedTasks workerTasks) {
        WorkerConfiguration workerConfiguration = workerConfigurationBuilder
                .withTransport(this)
                .withExecutorService(executorServiceSupplier.get())
                .build();

        currentWorker = new Worker(workerConfiguration);
        workerThread = new Thread(workersThreadGroup, () -> {
            try {
                currentWorker.run(workerTasks);
            } catch (InterruptedException | ExecutionException e) {
                logger.atSevere().withCause(e).log("Unhandled exception in worker thread");
            }
        });
        workerThread.start();
    }

    @Override
    public Map<TaskId, TaskState> getTaskStates(Set<TaskId> tasks) {
        return stateStore.loadTaskStates(tasks);
    }

    @Override
    public void setState(TaskId task, TaskState newState) {
        activeTasks.add(task);
        stateStore.saveTaskState(task, newState);
    }

    @Override
    public void updateState(TaskId task, TaskState newState) {
        if (!activeTasks.contains(task)) {
            throw new TaskAbortedException("Cannot update state for non-existent task: " + task);
        }
        stateStore.saveTaskState(task, newState);
    }

    @Override
    public void moveStateToNextWindow(TaskId task, TaskState newState) {
        if (!activeTasks.contains(task)) {
            throw new TaskAbortedException("Cannot update state for non-existent task: " + task);
        }
        stateStore.saveTaskState(task, newState);
    }

    private void stopWorkerThread() throws InterruptedException {
        if (currentWorker != null) {
            Worker workerToStop = currentWorker;
            Thread threadToJoin = workerThread;

            currentWorker = null;
            workerThread = null;

            workerToStop.stop();
            threadToJoin.join();
        }
    }

    public void stop() throws InterruptedException {
        stopWorkerThread();
    }

    public boolean isReadyToStart() {
        return currentWorker == null;
    }
}
