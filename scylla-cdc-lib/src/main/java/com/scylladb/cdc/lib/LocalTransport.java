package com.scylladb.cdc.lib;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
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
    private final WorkerConfiguration.Builder workerConfigurationBuilder;
    private final Supplier<ScheduledExecutorService> executorServiceSupplier;
    private final TaskStateBackend backend;

    private Optional<GenerationId> currentGenerationId;

    private WorkerHandle currentWorker;

    // Track generation IDs by table for tablet mode
    protected final Map<TableName, GenerationMetadata> currentGenerationByTable = new ConcurrentHashMap<>();

    public LocalTransport(ThreadGroup cdcThreadGroup, WorkerConfiguration.Builder workerConfigurationBuilder,
                          Supplier<ScheduledExecutorService> executorServiceSupplier) {
        this(cdcThreadGroup, workerConfigurationBuilder, executorServiceSupplier,
                new InProcessTaskStateBackend());
    }

    public LocalTransport(ThreadGroup cdcThreadGroup, WorkerConfiguration.Builder workerConfigurationBuilder,
                          Supplier<ScheduledExecutorService> executorServiceSupplier,
                          TaskStateBackend backend) {
        Preconditions.checkNotNull(cdcThreadGroup);
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
        GenerationMetadata metadata = currentGenerationByTable.get(tableName);
        if (metadata != null) {
            return Optional.of(metadata.getId());
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
    public synchronized void configureWorkers(GroupedTasks workerTasks)
            throws InterruptedException {
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
        stopCurrentWorker();

        // Create and start a new worker
        startNewWorker(workerTasks);
    }

    @Override
    public synchronized void configureWorkers(TableName tableName, GroupedTasks workerTasks)
            throws InterruptedException {
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

        // Update generation metadata for this table
        currentGenerationByTable.put(tableName, workerTasks.getGenerationMetadata());
        backend.saveGenerationId(tableName, workerTasks.getGenerationMetadata().getId());

        if (currentWorker == null) {
            // No worker exists, start a new one
            startNewWorker(workerTasks);
        } else {
            if (!tasks.isEmpty()) {
                try {
                    currentWorker.worker.addTasks(workerTasks);
                } catch (ExecutionException e) {
                    stopCurrentWorkerAfterFailure(e);
                    throw new RuntimeException("Error adding tasks", e);
                } catch (RuntimeException e) {
                    stopCurrentWorkerAfterFailure(e);
                    throw e;
                } catch (InterruptedException e) {
                    stopCurrentWorkerAfterInterruption();
                    throw e;
                }
            }
        }
    }

    @Override
    public synchronized void stopWorkers() throws InterruptedException {
        stopCurrentWorker();
    }

    private void startNewWorker(GroupedTasks workerTasks) throws InterruptedException {
        if (workerTasks.getTasks().isEmpty()) {
            return;
        }

        ScheduledExecutorService executor = Preconditions.checkNotNull(
                executorServiceSupplier.get(), "Worker executor cannot be null");
        Worker worker;
        try {
            WorkerConfiguration workerConfiguration = workerConfigurationBuilder
                    .withTransport(this)
                    .withExecutorService(executor)
                    .build();
            worker = new Worker(workerConfiguration);
        } catch (RuntimeException e) {
            executor.shutdownNow();
            throw e;
        }

        WorkerHandle workerHandle = new WorkerHandle(worker, executor);
        try {
            worker.addTasks(workerTasks);
            currentWorker = workerHandle;
        } catch (ExecutionException e) {
            stopWorkerAfterFailure(workerHandle, e);
            throw new RuntimeException("Error starting worker", e);
        } catch (RuntimeException e) {
            stopWorkerAfterFailure(workerHandle, e);
            throw e;
        } catch (InterruptedException e) {
            stopWorkerAfterInterruption(workerHandle);
            throw e;
        }
    }

    @Override
    public Map<TaskId, TaskState> getTaskStates(Set<TaskId> tasks) {
        return backend.getTaskStates(tasks);
    }

    @Override
    public Map<TaskId, TaskState> getTaskStatesForMigration(Set<TaskId> tasks) {
        return backend.getTaskStates(tasks);
    }

    @Override
    public void completeTaskStateMigration(Set<TaskId> legacyTasks) {
        // LocalTransport receives the complete table assignment from the tablet master. The
        // default stateless coordinator therefore reaches completion only after this worker has
        // persisted every replacement in the authoritative coordination group. Worker handles a
        // deletion failure without interrupting consumption and retries it on the next start.
        backend.deleteTasks(legacyTasks);
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

    private void stopCurrentWorker() throws InterruptedException {
        WorkerHandle workerToStop = currentWorker;
        currentWorker = null;
        if (workerToStop != null) {
            stopWorker(workerToStop);
        }
    }

    private void stopCurrentWorkerAfterFailure(Exception failure)
            throws InterruptedException {
        WorkerHandle workerToStop = currentWorker;
        currentWorker = null;
        if (workerToStop != null) {
            stopWorkerAfterFailure(workerToStop, failure);
        }
    }

    private void stopCurrentWorkerAfterInterruption() {
        WorkerHandle workerToStop = currentWorker;
        currentWorker = null;
        if (workerToStop != null) {
            stopWorkerAfterInterruption(workerToStop);
        }
    }

    private static void stopWorkerAfterFailure(WorkerHandle workerHandle, Exception failure)
            throws InterruptedException {
        try {
            stopWorker(workerHandle);
        } catch (InterruptedException e) {
            e.addSuppressed(failure);
            throw e;
        }
    }

    private static void stopWorkerAfterInterruption(WorkerHandle workerHandle) {
        workerHandle.worker.stop();
        workerHandle.executor.shutdownNow();
    }

    private static void stopWorker(WorkerHandle workerHandle) throws InterruptedException {
        workerHandle.worker.stop();
        try {
            while (!workerHandle.executor.awaitTermination(Long.MAX_VALUE,
                    TimeUnit.NANOSECONDS)) {
                // Keep waiting until every worker action has stopped.
            }
        } catch (InterruptedException e) {
            workerHandle.executor.shutdownNow();
            throw e;
        }
    }

    public synchronized void stop() throws InterruptedException {
        stopCurrentWorker();
    }

    public synchronized boolean isReadyToStart() {
        return currentWorker == null;
    }

    private static final class WorkerHandle {
        private final Worker worker;
        private final ScheduledExecutorService executor;

        private WorkerHandle(Worker worker, ScheduledExecutorService executor) {
            this.worker = worker;
            this.executor = executor;
        }
    }
}
