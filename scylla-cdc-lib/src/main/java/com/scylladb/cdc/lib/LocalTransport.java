package com.scylladb.cdc.lib;

import java.util.HashMap;
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
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.master.GenerationMetadata;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.TaskState;
import com.scylladb.cdc.model.worker.Worker;
import com.scylladb.cdc.model.worker.WorkerConfiguration;
import com.scylladb.cdc.transport.MasterTransport;
import com.scylladb.cdc.transport.TaskAbortedException;
import com.scylladb.cdc.transport.GroupedTasks;
import com.scylladb.cdc.transport.WorkerTransport;

class LocalTransport implements MasterTransport, WorkerTransport {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final ThreadGroup workersThreadGroup;
    private final WorkerConfiguration.Builder workerConfigurationBuilder;
    private final ConcurrentHashMap<TaskId, TaskState> taskStates = new ConcurrentHashMap<>();
    private final Supplier<ScheduledExecutorService> executorServiceSupplier;
    private Optional<GenerationId> currentGenerationId = Optional.empty();

    // Single worker reference
    private Worker currentWorker = null;
    private Thread workerThread = null;

    // Track generation IDs by table for tablet mode
    protected final Map<TableName, GenerationMetadata> currentGenerationByTable = new ConcurrentHashMap<>();

    public LocalTransport(ThreadGroup cdcThreadGroup, WorkerConfiguration.Builder workerConfigurationBuilder,
                          Supplier<ScheduledExecutorService> executorServiceSupplier) {
        workersThreadGroup = new ThreadGroup(cdcThreadGroup, "Scylla-CDC-Worker-Threads");
        this.workerConfigurationBuilder = Preconditions.checkNotNull(workerConfigurationBuilder);
        this.executorServiceSupplier = Preconditions.checkNotNull(executorServiceSupplier);
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
        if (taskStates.isEmpty()) {
            return false;
        }
        for (TaskId id : tasks) {
            TaskState state = taskStates.get(id);
            if (state == null || !state.hasPassed(until)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void configureWorkers(GroupedTasks workerTasks) throws InterruptedException {
        Map<TaskId, SortedSet<StreamId>> tasks = workerTasks.getTasks();

        // Remove task states for tasks no longer in the configuration
        Iterator<TaskId> it = taskStates.keySet().iterator();
        while (it.hasNext()) {
            if (!tasks.containsKey(it.next())) {
                it.remove();
            }
        }

        currentGenerationId = Optional.ofNullable(workerTasks.getGenerationId());

        // Stop current worker if exists
        stopWorkerThread();

        // Create and start a new worker
        startNewWorkerThread(workerTasks);
    }

    @Override
    public void configureWorkers(TableName tableName, GroupedTasks workerTasks) throws InterruptedException {
        Map<TaskId, SortedSet<StreamId>> tasks = workerTasks.getTasks();

        // Remove all existing tasks from taskStates that belong to this table and no longer in the configuration
        Iterator<TaskId> it = taskStates.keySet().iterator();
        while (it.hasNext()) {
            TaskId taskId = it.next();
            if (taskId.getTable().equals(tableName) && !tasks.containsKey(taskId)) {
                it.remove();
            }
        }

        // Update generation metadata for this table
        currentGenerationByTable.put(tableName, workerTasks.getGenerationMetadata());

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
        Map<TaskId, TaskState> result = new HashMap<>();
        tasks.forEach(task -> {
            TaskState taskState = taskStates.get(task);
            if (taskState != null) {
                result.put(task, taskState);
            }
        });
        return result;
    }

    @Override
    public void setState(TaskId task, TaskState newState) {
        taskStates.put(task, newState);
    }

    @Override
    public void updateState(TaskId task, TaskState newState) {
        if (taskStates.replace(task, newState) == null) {
            throw new TaskAbortedException("Cannot update state for non-existent task: " + task);
        }
    }

    @Override
    public void moveStateToNextWindow(TaskId task, TaskState newState) {
        if (taskStates.replace(task, newState) == null) {
            throw new TaskAbortedException("Cannot update state for non-existent task: " + task);
        }
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
