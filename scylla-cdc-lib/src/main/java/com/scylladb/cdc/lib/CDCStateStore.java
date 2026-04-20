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
 * Pluggable storage backend for CDC consumer checkpoint state.
 *
 * <p>Implementations persist two kinds of state:
 * <ol>
 *   <li><b>Task states</b> — per-task read progress ({@link TaskState}). The worker reads these on
 *       startup to resume from the last checkpoint, and writes them after every consumed CDC change
 *       and on every time-window advancement. Implementations should be efficient for high-frequency
 *       writes.
 *   <li><b>Generation IDs</b> — the current CDC generation being processed (vnode-based and
 *       per-table for tablet-based CDC). The master reads these on startup so it can resume from the
 *       correct generation rather than re-scanning from the beginning.
 * </ol>
 *
 * <p><b>Thread safety:</b> All methods will be called concurrently from the master thread and one
 * or more worker threads. Implementations must be thread-safe.
 *
 * <p><b>Failure handling:</b> If a write fails, implementations should throw a
 * {@link RuntimeException}. The library will treat this as a fatal error for the affected task.
 * At-least-once delivery is guaranteed — if a state write fails and the process restarts, the
 * last successfully written checkpoint will be used, meaning some changes may be re-delivered.
 *
 * <p><b>Default implementation:</b> {@link InMemoryStateStore} — stores state in memory with no
 * persistence. Suitable for development or stateless pipelines. For production use cases that
 * require resuming after restart, provide a persistent implementation.
 *
 * <p><b>Usage with {@link CDCConsumer}:</b>
 * <pre>{@code
 * CDCStateStore store = new RedisStateStore(jedisPool);  // or your own implementation
 *
 * try (CDCConsumer consumer = CDCConsumer.builder()
 *         .addContactPoint("scylla-node1")
 *         .addTable("my_keyspace", "my_table")
 *         .withConsumer(change -> { /* process change *\/ })
 *         .withStateStore(store)
 *         .build()) {
 *     consumer.start();
 * }
 * }</pre>
 *
 * @see InMemoryStateStore
 * @see TaskStateSerde
 */
public interface CDCStateStore {

    // -------------------------------------------------------------------------
    // Task state (checkpoint) operations
    // -------------------------------------------------------------------------

    /**
     * Loads previously saved task states for the given set of task IDs.
     *
     * <p>Called by the worker on startup to restore checkpoints. Only tasks that have a saved state
     * need to be returned — tasks absent from the returned map will start reading from the beginning
     * of their generation.
     *
     * <p>Implementations may return states for a subset of the requested tasks (e.g., on first
     * startup when no state has been saved yet).
     *
     * @param tasks the set of task IDs for which to load state
     * @return a map from task ID to saved state; may be empty but never null
     */
    Map<TaskId, TaskState> loadTaskStates(Set<TaskId> tasks);

    /**
     * Saves or replaces the state for a single task.
     *
     * <p>Called by the worker to initialize a new task's state before it starts processing, and
     * subsequently on every change consumed and every time-window advancement. This is on the hot
     * path — implementations should complete quickly.
     *
     * @param task     the task ID whose state is being saved
     * @param state    the new state to persist
     */
    void saveTaskState(TaskId task, TaskState state);

    /**
     * Deletes saved states for tasks that are no longer active.
     *
     * <p>Called when transitioning between CDC generations: the old generation's tasks are pruned
     * before the new generation's tasks are started. Implementations may silently ignore task IDs
     * that have no saved state.
     *
     * @param tasks the set of task IDs whose state should be deleted
     */
    void deleteTaskStates(Set<TaskId> tasks);

    // -------------------------------------------------------------------------
    // Generation tracking (vnode-based CDC)
    // -------------------------------------------------------------------------

    /**
     * Loads the current vnode-based CDC generation ID.
     *
     * <p>Called by the master on startup. If a generation ID is returned, the master resumes from
     * that generation rather than scanning from the very first generation. Returns empty if no
     * generation has been saved (first startup).
     *
     * @return the current generation ID, or empty if not saved
     */
    Optional<GenerationId> loadGenerationId();

    /**
     * Saves the current vnode-based CDC generation ID.
     *
     * <p>Called by the master when it transitions to a new generation.
     *
     * @param generationId the generation ID to persist
     */
    void saveGenerationId(GenerationId generationId);

    // -------------------------------------------------------------------------
    // Generation tracking (tablet-based CDC)
    // -------------------------------------------------------------------------

    /**
     * Loads the current CDC generation ID for a specific table (tablet-based CDC).
     *
     * <p>In tablet-based CDC mode, each table has its own independent generation progression.
     * Returns empty if no generation has been saved for this table.
     *
     * @param table the table for which to load the generation ID
     * @return the current generation ID for the table, or empty if not saved
     */
    Optional<GenerationId> loadGenerationId(TableName table);

    /**
     * Saves the current CDC generation ID for a specific table (tablet-based CDC).
     *
     * @param table        the table for which to save the generation ID
     * @param generationId the generation ID to persist
     */
    void saveGenerationId(TableName table, GenerationId generationId);

    /**
     * Checks whether all given tasks have progressed past the specified timestamp.
     *
     * <p>Called by the master to determine if a CDC generation is fully consumed before advancing
     * to the next one. The default implementation loads task states and checks each one, which
     * may result in multiple reads for large task sets.
     *
     * <p>Implementations backed by a SQL database or Redis may override this to issue a single
     * query (e.g., {@code COUNT(*) WHERE window_start <= timestamp}) for better performance.
     *
     * @param tasks the set of task IDs to check
     * @param until the timestamp boundary; all tasks must have {@code windowStart > until}
     * @return true if all tasks have progressed past {@code until}, false otherwise
     */
    default boolean areTasksFullyConsumedUntil(Set<TaskId> tasks, Timestamp until) {
        Map<TaskId, TaskState> states = loadTaskStates(tasks);
        for (TaskId id : tasks) {
            TaskState state = states.get(id);
            if (state == null || !state.hasPassed(until)) {
                return false;
            }
        }
        return true;
    }
}
