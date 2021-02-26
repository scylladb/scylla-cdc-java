package com.scylladb.cdc.lib;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.driver3.Driver3Session;
import com.scylladb.cdc.cql.driver3.Driver3WorkerCQL;
import com.scylladb.cdc.model.ExponentialRetryBackoffWithJitter;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.RetryBackoff;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import com.scylladb.cdc.model.worker.WorkerConfiguration;
import com.scylladb.cdc.model.worker.TaskAndRawChangeConsumerAdapter;
import com.scylladb.cdc.model.worker.TaskState;
import com.scylladb.cdc.transport.MasterTransport;
import com.scylladb.cdc.transport.WorkerTransport;

public class LocalTransport implements MasterTransport, WorkerTransport {
    private final ThreadGroup workersThreadGroup;
    private volatile boolean stopped = true;
    private final WorkerConfiguration.Builder workerConfigurationBuilder;
    private final RawChangeConsumerProvider consumerProvider;
    private final ConcurrentHashMap<TaskId, TaskState> taskStates = new ConcurrentHashMap<>();
    private volatile int workersCount;
    private Thread[] workerThreads;

    public LocalTransport(ThreadGroup cdcThreadGroup, int workersCount, WorkerConfiguration.Builder workerConfigurationBuilder, RawChangeConsumerProvider consumerProvider) {
        Preconditions.checkArgument(workersCount > 0);
        this.workersCount = workersCount;
        workersThreadGroup = new ThreadGroup(cdcThreadGroup, "Scylla-CDC-Worker-Threads");
        this.workerConfigurationBuilder = Preconditions.checkNotNull(workerConfigurationBuilder);
        this.consumerProvider = Preconditions.checkNotNull(consumerProvider);
    }

    @Override
    public Optional<GenerationId> getCurrentGenerationId() {
        return Optional.empty();
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
    public void configureWorkers(Map<TaskId, SortedSet<StreamId>> workerConfigurations) throws InterruptedException {
        Iterator<TaskId> it = taskStates.keySet().iterator();
        while (it.hasNext()) {
            if (!workerConfigurations.containsKey(it.next())) {
                it.remove();
            }
        }
        stop();
        stopped = false;
        int wCount = Math.min(workersCount, workerConfigurations.size());
        workerThreads = new Thread[wCount];
        Map<TaskId, SortedSet<StreamId>>[] tasks = split(workerConfigurations, wCount);
        for (int i = 0; i < wCount; ++i) {
            WorkerConfiguration workerConfiguration =
                    workerConfigurationBuilder
                            .withTransport(this)
                            .withConsumer(new TaskAndRawChangeConsumerAdapter(consumerProvider.getForThread(i)))
                            .build();

            workerThreads[i] = new WorkerThread(workersThreadGroup, i, workerConfiguration, tasks[i]);
            workerThreads[i].start();
        }
    }

    private static Map<TaskId, SortedSet<StreamId>>[] split(Map<TaskId, SortedSet<StreamId>> tasks, int wCount) {
        @SuppressWarnings("unchecked")
        Map<TaskId, SortedSet<StreamId>>[] result = new Map[wCount];
        for (int i = 0; i < wCount; ++i) {
            result[i] = new HashMap<>();
        }
        int pos = 0;
        for (Map.Entry<TaskId, SortedSet<StreamId>> e : tasks.entrySet()) {
            result[pos].put(e.getKey(), e.getValue());
            pos = (pos + 1) % wCount;
        }
        return result;
    }

    @Override
    public Map<TaskId, TaskState> getTaskStates(Set<TaskId> tasks) {
        return new HashMap<>();
    }

    @Override
    public void setState(TaskId task, TaskState newState) {
        taskStates.put(task, newState);
    }

    @Override
    public void moveStateToNextWindow(TaskId task, TaskState newState) {
        taskStates.put(task, newState);
    }

    @Override
    public boolean shouldStop() {
        return stopped;
    }

    public void stop() throws InterruptedException {
        stopped = true;
        if (workerThreads != null) {
            for (Thread t : workerThreads) {
                t.join();
            }
        }
        workerThreads = null;
    }

    public void setWorkersCount(int count) {
        workersCount = count;
    }

    public boolean isReadyToStart() {
        return workerThreads == null;
    }

}
