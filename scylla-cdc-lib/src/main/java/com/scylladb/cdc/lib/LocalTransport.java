package com.scylladb.cdc.lib;

import java.util.HashSet;
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
    private final TaskStateBackend backend;

    private Optional<GenerationId> currentGenerationId;

    // Single worker reference
    private Worker currentWorker = null;
    private Thread workerThread = null;

    // Track generation IDs by table for tablet mode
    protected final Map<TableName, GenerationId> currentGenerationByTable = new ConcurrentHashMap<>();

    public LocalTransport(ThreadGroup cdcThreadGroup, WorkerConfiguration.Builder workerConfigurationBuilder,
                          Supplier<ScheduledExecutorService> executorServiceSupplier) {
        this(cdcThreadGroup, workerConfigurationBuilder, executorServiceSupplier,
                new InProcessTaskStateBackend());
    }

    public LocalTransport(ThreadGroup cdcThreadGroup, WorkerConfiguration.Builder workerConfigurationBuilder,
                          Supplier<ScheduledExecutorService> executorServiceSupplier,
                          TaskStateBackend backend) {
        workersThreadGroup = new ThreadGroup(cdcThreadGroup, "Scylla-CDC-Worker-Threads");
        this.workerConfigurationBuilder = Preconditions.checkNotNull(workerConfigurationBuilder);
        this.executorServiceSupplier = Preconditions.checkNotNull(executorServiceSupplier);
        this.backend = Preconditions.checkNotNull(backend);
        this.currentGenerationId = backend.loadGenerationId();
    }

    @Override
    public Optional<GenerationId> getCurrentGenerationId() {
        return currentGenerationId;
    }

    @Override
    public Optional<GenerationId> getCurrentGenerationId(TableName tableName) {
        GenerationId generationId = currentGenerationByTable.get(tableName);
        if (generationId != null) {
            return Optional.of(generationId);
        }
        // Fall back to persisted generation ID from a previous run (before configureWorkers
        // has populated currentGenerationByTable for this table in the current run).
        return backend.loadGenerationId(tableName);
    }

    @Override
    public boolean areTasksFullyConsumedUntil(Set<TaskId> tasks, Timestamp until) {
        return backend.areTasksFullyConsumedUntil(tasks, until);
    }

    @Override
    public void configureWorkers(GroupedTasks workerTasks) throws InterruptedException {
        Map<TaskId, SortedSet<StreamId>> tasks = workerTasks.getTasks();

        // Determine which tasks are being removed and clean them up
        Set<TaskId> toDelete = new HashSet<>(backend.getActiveTasks());
        toDelete.removeAll(tasks.keySet());
        if (!toDelete.isEmpty()) {
            backend.deleteTasks(toDelete);
        }

        currentGenerationId = Optional.ofNullable(workerTasks.getGenerationId());
        if (workerTasks.getGenerationId() != null) {
            backend.saveGenerationId(workerTasks.getGenerationId());
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
        for (TaskId taskId : backend.getActiveTasks()) {
            if (taskId.getTable().equals(tableName) && !tasks.containsKey(taskId)) {
                toDelete.add(taskId);
            }
        }
        if (!toDelete.isEmpty()) {
            backend.deleteTasks(toDelete);
        }

        // Update generation ID for this table
        currentGenerationByTable.put(tableName, workerTasks.getGenerationId());
        backend.saveGenerationId(tableName, workerTasks.getGenerationId());

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
        return backend.getTaskStates(tasks);
    }

    @Override
    public void setState(TaskId task, TaskState newState) {
        backend.setState(task, newState);
    }

    @Override
    public void updateState(TaskId task, TaskState newState) {
        if (!backend.replaceState(task, newState)) {
            throw new TaskAbortedException("Cannot update state for non-existent task: " + task);
        }
    }

    @Override
    public void moveStateToNextWindow(TaskId task, TaskState newState) {
        if (!backend.replaceState(task, newState)) {
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
