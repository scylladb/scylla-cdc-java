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
 * <p>This is the default implementation used by {@link CDCConsumer} when no custom store is
 * configured. All state is stored in {@link ConcurrentHashMap} instances and is lost when the
 * process exits.
 *
 * <p>This implementation is suitable for:
 * <ul>
 *   <li>Development and testing
 *   <li>Stateless pipelines where replaying from the beginning of the CDC log on restart is
 *       acceptable
 *   <li>Short-lived consumers
 * </ul>
 *
 * <p>For production use cases that require resuming from the last checkpoint after a process
 * restart, use a persistent implementation such as {@code RedisStateStore} from the
 * {@code scylla-cdc-state-redis} module, or implement {@link CDCStateStore} with your own backend.
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
