package com.scylladb.cdc.lib;

import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.worker.TaskState;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Package-private strategy interface for task state storage within {@link LocalTransport}.
 *
 * <p>Two implementations are provided:
 * <ul>
 *   <li>{@link InProcessTaskStateBackend} — in-process {@link java.util.concurrent.ConcurrentHashMap}
 *       (original behavior; used when no {@link CDCStateStore} is configured).
 *   <li>{@link StoreBackedTaskStateBackend} — delegates to a {@link CDCStateStore} instance
 *       (used when {@link CDCConsumer.Builder#withStateStore} is called).
 * </ul>
 *
 * <p>The backend is selected in {@link CDCConsumer.Builder#build()} based on whether a
 * {@link CDCStateStore} has been provided.
 */
interface TaskStateBackend {

    /**
     * Returns saved states for the given task IDs. Tasks with no saved state are absent from
     * the returned map.
     */
    Map<TaskId, TaskState> getTaskStates(Set<TaskId> tasks);

    /**
     * Records a task as active and saves its initial state.
     */
    void setState(TaskId task, TaskState state);

    /**
     * Saves a new state for a task that is already active.
     *
     * @return {@code true} if the task was active and the state was saved;
     *         {@code false} if the task was not active (signals {@link com.scylladb.cdc.transport.TaskAbortedException})
     */
    boolean replaceState(TaskId task, TaskState newState);

    /**
     * Returns the set of all currently active task IDs (those registered via {@link #setState}
     * and not yet removed by {@link #deleteTasks}).
     *
     * <p>Used by {@link LocalTransport#configureWorkers} to compute which tasks have been dropped
     * from the new generation and must be deleted.
     */
    Set<TaskId> getActiveTasks();

    /**
     * Removes the given tasks from the active set and purges their saved state.
     */
    void deleteTasks(Set<TaskId> tasks);

    /**
     * Returns {@code true} if all given tasks have progressed past {@code until}.
     */
    boolean areTasksFullyConsumedUntil(Set<TaskId> tasks, Timestamp until);

    /**
     * Loads the persisted vnode-based CDC generation ID from a previous run.
     *
     * <p>Called during {@link LocalTransport} construction to pre-populate
     * {@code currentGenerationId}, allowing the master to skip already-consumed generations
     * on restart without N sequential CQL round-trips.
     *
     * <p>Returns {@link Optional#empty()} for the in-process backend (preserves original
     * master behavior of always discovering generations from scratch).
     */
    Optional<GenerationId> loadGenerationId();

    /**
     * Loads the persisted CDC generation ID for a specific table (tablet-based CDC).
     *
     * <p>Called lazily by {@link LocalTransport#getCurrentGenerationId(TableName)} when the
     * in-process {@code currentGenerationByTable} map does not yet have an entry for the table
     * (i.e. on the first call after startup, before {@code configureWorkers} has run).
     *
     * <p>Returns {@link Optional#empty()} for the in-process backend.
     */
    Optional<GenerationId> loadGenerationId(TableName table);

    /**
     * Persists the current vnode-based CDC generation ID (no-op for in-process backend).
     */
    void saveGenerationId(GenerationId generationId);

    /**
     * Persists the current CDC generation ID for a specific table (no-op for in-process backend).
     */
    void saveGenerationId(TableName table, GenerationId generationId);
}
