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
 * In-memory implementation of {@link CDCStateStore}.
 *
 * <p>All state is stored in {@link ConcurrentHashMap} instances and is lost when the
 * process exits. This implementation is suitable for:
 * <ul>
 *   <li>Development and testing
 *   <li>Stateless pipelines where replaying from the beginning of the CDC log on restart is
 *       acceptable
 *   <li>Short-lived consumers
 * </ul>
 *
 * <p>Pass an instance explicitly to {@link CDCConsumer.Builder#withStateStore} if you want
 * in-memory semantics with the {@link CDCStateStore} API (e.g. for tests or mocking). When no
 * store is configured at all, {@link CDCConsumer} uses the original in-process
 * {@link java.util.concurrent.ConcurrentHashMap}-based codepath directly.
 *
 * <p>For production use cases that require resuming from the last checkpoint after a process
 * restart, provide a persistent implementation backed by Redis, a SQL database, or any other
 * durable store, and implement {@link CDCStateStore} accordingly.
 */
public class InMemoryStateStore implements CDCStateStore {

    private final ConcurrentHashMap<TaskId, TaskState> taskStates = new ConcurrentHashMap<>();
    private volatile GenerationId currentGenerationId = null;
    private final ConcurrentHashMap<TableName, GenerationId> tableGenerationIds = new ConcurrentHashMap<>();

    @Override
    public Map<TaskId, TaskState> loadTaskStates(Set<TaskId> tasks) {
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
    public void saveTaskState(TaskId task, TaskState state) {
        taskStates.put(task, state);
    }

    @Override
    public void deleteTaskStates(Set<TaskId> tasks) {
        for (TaskId task : tasks) {
            taskStates.remove(task);
        }
    }

    @Override
    public Optional<GenerationId> loadGenerationId() {
        return Optional.ofNullable(currentGenerationId);
    }

    @Override
    public void saveGenerationId(GenerationId generationId) {
        this.currentGenerationId = generationId;
    }

    @Override
    public Optional<GenerationId> loadGenerationId(TableName table) {
        return Optional.ofNullable(tableGenerationIds.get(table));
    }

    @Override
    public void saveGenerationId(TableName table, GenerationId generationId) {
        tableGenerationIds.put(table, generationId);
    }

    /**
     * Overrides the default to avoid a redundant {@link #loadTaskStates} call: checks directly
     * against the in-memory map.
     */
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
}
