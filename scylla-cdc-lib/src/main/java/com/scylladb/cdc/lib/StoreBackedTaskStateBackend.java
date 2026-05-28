package com.scylladb.cdc.lib;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.worker.TaskState;
import com.scylladb.cdc.transport.TaskAbortedException;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link TaskStateBackend} that delegates all state operations to a {@link CDCStateStore}.
 *
 * <p>This backend is used by {@link LocalTransport} when a {@link CDCStateStore} has been
 * configured via {@link CDCConsumer.Builder#withStateStore}. It enables durable checkpoint
 * persistence — the consumer can resume from the last written checkpoint after a process restart.
 *
 * <p>An in-process {@code activeTasks} set is maintained alongside the store to detect
 * {@link TaskAbortedException} without requiring a store read. This is safe with
 * eventually-consistent backends because task activation ({@link #setState}) and deactivation
 * ({@link #deleteTasks}) are always driven by the master thread via {@code configureWorkers},
 * which runs single-threaded relative to task lifecycle changes.
 */
class StoreBackedTaskStateBackend implements TaskStateBackend {

    private final CDCStateStore stateStore;

    /**
     * Tracks which task IDs are currently active (registered via {@link #setState}).
     * Used to detect {@link TaskAbortedException} in {@link #replaceState} without a store read.
     */
    private final Set<TaskId> activeTasks = Collections.newSetFromMap(new ConcurrentHashMap<>());

    StoreBackedTaskStateBackend(CDCStateStore stateStore) {
        this.stateStore = Preconditions.checkNotNull(stateStore, "stateStore must not be null");
    }

    @Override
    public Map<TaskId, TaskState> getTaskStates(Set<TaskId> tasks) {
        return stateStore.loadTaskStates(tasks);
    }

    @Override
    public void setState(TaskId task, TaskState state) {
        activeTasks.add(task);
        stateStore.saveTaskState(task, state);
    }

    @Override
    public boolean replaceState(TaskId task, TaskState newState) {
        if (!activeTasks.contains(task)) {
            return false;
        }
        stateStore.saveTaskState(task, newState);
        return true;
    }

    @Override
    public Set<TaskId> getActiveTasks() {
        return Collections.unmodifiableSet(activeTasks);
    }

    @Override
    public void deleteTasks(Set<TaskId> tasks) {
        if (tasks.isEmpty()) {
            return;
        }
        activeTasks.removeAll(tasks);
        stateStore.deleteTaskStates(tasks);
    }

    @Override
    public boolean areTasksFullyConsumedUntil(Set<TaskId> tasks, Timestamp until) {
        return stateStore.areTasksFullyConsumedUntil(tasks, until);
    }

    @Override
    public Optional<GenerationId> loadGenerationId() {
        return stateStore.loadGenerationId();
    }

    @Override
    public Optional<GenerationId> loadGenerationId(TableName table) {
        return stateStore.loadGenerationId(table);
    }

    @Override
    public void saveGenerationId(GenerationId generationId) {
        stateStore.saveGenerationId(generationId);
    }

    @Override
    public void saveGenerationId(TableName table, GenerationId generationId) {
        stateStore.saveGenerationId(table, generationId);
    }
}
