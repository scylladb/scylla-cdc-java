package com.scylladb.cdc.lib;

import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.worker.TaskState;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-process {@link TaskStateBackend} implementation.
 *
 * <p>This is the default backend used by {@link LocalTransport} when no {@link CDCStateStore}
 * has been configured. It replicates the original pre-store {@code LocalTransport} behavior
 * exactly: task states are held in a {@link ConcurrentHashMap} and are lost on process restart.
 *
 * <p>Generation IDs are not persisted — {@link #saveGenerationId} is a no-op.
 */
class InProcessTaskStateBackend implements TaskStateBackend {

    private final ConcurrentHashMap<TaskId, TaskState> taskStates = new ConcurrentHashMap<>();

    @Override
    public Map<TaskId, TaskState> getTaskStates(Set<TaskId> tasks) {
        Map<TaskId, TaskState> result = new HashMap<>();
        for (TaskId task : tasks) {
            TaskState state = taskStates.get(task);
            if (state != null) {
                result.put(task, state);
            }
        }
        return result;
    }

    @Override
    public void setState(TaskId task, TaskState state) {
        taskStates.put(task, state);
    }

    @Override
    public boolean replaceState(TaskId task, TaskState newState) {
        return taskStates.replace(task, newState) != null;
    }

    @Override
    public Set<TaskId> getActiveTasks() {
        return taskStates.keySet();
    }

    @Override
    public void deleteTasks(Set<TaskId> tasks) {
        for (TaskId task : tasks) {
            taskStates.remove(task);
        }
    }

    @Override
    public boolean areTasksFullyConsumedUntil(Set<TaskId> tasks, Timestamp until) {
        for (TaskId id : tasks) {
            TaskState state = taskStates.get(id);
            if (state == null || !state.hasPassed(until)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Optional<GenerationId> loadGenerationId() {
        // No-op: in-process backend has no persistence; preserves original master behavior
        // of always returning empty (master discovers generations from scratch on startup).
        return Optional.empty();
    }

    @Override
    public Optional<GenerationId> loadGenerationId(TableName table) {
        return Optional.empty();
    }

    @Override
    public void saveGenerationId(GenerationId generationId) {
        // No-op: generation IDs are not persisted by the in-process backend.
    }

    @Override
    public void saveGenerationId(TableName table, GenerationId generationId) {
        // No-op: generation IDs are not persisted by the in-process backend.
    }
}
