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
import com.scylladb.cdc.model.worker.TaskState;
import com.scylladb.cdc.model.worker.Worker;
import com.scylladb.cdc.model.worker.WorkerConfiguration;
import com.scylladb.cdc.transport.MasterTransport;
import com.scylladb.cdc.transport.GroupedTasks;
import com.scylladb.cdc.transport.WorkerTransport;

class LocalTransport implements MasterTransport, WorkerTransport {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final ThreadGroup workersThreadGroup;
    private final WorkerConfiguration.Builder workerConfigurationBuilder;
    private final ConcurrentHashMap<TaskId, TaskState> taskStates = new ConcurrentHashMap<>();
    private final Supplier<ScheduledExecutorService> executorServiceSupplier;
    private Optional<GenerationId> currentGenerationId = Optional.empty();
    private Supplier<InterruptedException> stopWorker = null;

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
        stop();

        WorkerConfiguration workerConfiguration = workerConfigurationBuilder
                .withTransport(this)
                .withExecutorService(executorServiceSupplier.get())
                .build();

        Worker w = new Worker(workerConfiguration);
        Thread t = new Thread(workersThreadGroup, () -> {
            try {
                w.run(workerTasks);
            } catch (InterruptedException | ExecutionException e) {
                logger.atSevere().withCause(e).log("Unhandled exception");
            } 
        });
        stopWorker = () -> {
            w.stop();
            try {
                t.join();
            } catch (InterruptedException e) {
                return e;
            }
            return null;
        };
        t.start();
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
    public void moveStateToNextWindow(TaskId task, TaskState newState) {
        taskStates.put(task, newState);
    }

    public void stop() throws InterruptedException {
        Supplier<InterruptedException> s = stopWorker;
        stopWorker = null;
        if (s != null) {
            InterruptedException e = s.get();
            if (e != null) {
                throw e;
            }
        }
    }

    public boolean isReadyToStart() {
        return stopWorker == null;
    }

}
