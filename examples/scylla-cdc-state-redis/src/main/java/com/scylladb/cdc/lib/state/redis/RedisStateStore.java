package com.scylladb.cdc.lib.state.redis;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.lib.CDCStateStore;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.TaskState;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Redis-backed implementation of {@link CDCStateStore}.
 *
 * <p>All state is persisted in Redis using the following key scheme:
 * <ul>
 *   <li>Task state: {@code <keyPrefix>:task:<taskIdKey>} — stored as a Redis hash with fields
 *       matching {@link TaskStateSerde} map keys ({@code window_start}, {@code window_end},
 *       {@code change_id_stream}, {@code change_id_time}).
 *   <li>Global generation ID: {@code <keyPrefix>:gen} — stored as a Redis string (epoch ms).
 *   <li>Per-table generation ID: {@code <keyPrefix>:gen:<keyspace>.<table>} — stored as a Redis
 *       string (epoch ms).
 * </ul>
 *
 * <p><b>Single-instance only:</b> This implementation is designed for a single
 * {@link com.scylladb.cdc.lib.CDCConsumer} instance. Running multiple instances against the same
 * key prefix concurrently is not supported and will result in state corruption. For horizontal
 * scaling, use the Scylla CDC Source Connector for Kafka Connect instead.
 *
 * <p><b>At-least-once delivery:</b> If a Redis write fails (e.g., connection loss), a
 * {@link RuntimeException} is thrown and the task is considered failed. On restart, the consumer
 * resumes from the last successfully written checkpoint, so some changes may be re-delivered.
 *
 * <h2>Usage example</h2>
 * <pre>{@code
 * JedisPool pool = new JedisPool("redis-host", 6379);
 * CDCStateStore store = new RedisStateStore(pool);
 *
 * try (CDCConsumer consumer = CDCConsumer.builder()
 *         .addContactPoint("scylla-node")
 *         .addTable("my_keyspace", "my_table")
 *         .withConsumer(change -> processChange(change))
 *         .withStateStore(store)
 *         .build()) {
 *     consumer.start();
 *     // consumer resumes from last checkpoint on restart
 * }
 * }</pre>
 */
public class RedisStateStore implements CDCStateStore {

    /** Default prefix for all Redis keys managed by this store. */
    public static final String DEFAULT_KEY_PREFIX = "scylla-cdc";

    private static final String TASK_KEY_SEGMENT = "task";
    private static final String GEN_KEY_SEGMENT = "gen";

    private final JedisPool jedisPool;
    private final String keyPrefix;

    /**
     * Creates a {@code RedisStateStore} using the given Jedis pool and the
     * {@link #DEFAULT_KEY_PREFIX default key prefix}.
     *
     * @param jedisPool the Jedis connection pool; must not be null
     */
    public RedisStateStore(JedisPool jedisPool) {
        this(jedisPool, DEFAULT_KEY_PREFIX);
    }

    /**
     * Creates a {@code RedisStateStore} using the given Jedis pool and a custom key prefix.
     *
     * <p>The key prefix is prepended to every Redis key, allowing multiple logical stores
     * (e.g. for different table sets) to share the same Redis instance without key collisions.
     *
     * @param jedisPool the Jedis connection pool; must not be null
     * @param keyPrefix the prefix to prepend to all Redis keys; must not be null or empty
     */
    public RedisStateStore(JedisPool jedisPool, String keyPrefix) {
        this.jedisPool = Preconditions.checkNotNull(jedisPool, "jedisPool must not be null");
        Preconditions.checkArgument(keyPrefix != null && !keyPrefix.isEmpty(),
                "keyPrefix must not be null or empty");
        this.keyPrefix = keyPrefix;
    }

    // -------------------------------------------------------------------------
    // Task state operations
    // -------------------------------------------------------------------------

    @Override
    public Map<TaskId, TaskState> loadTaskStates(Set<TaskId> tasks) {
        if (tasks.isEmpty()) {
            return new HashMap<>();
        }
        // Use a pipeline to batch all HGETALL commands into a single round-trip.
        List<TaskId> taskList = new ArrayList<>(tasks);
        List<Response<Map<String, String>>> responses = new ArrayList<>(taskList.size());
        try (var jedis = jedisPool.getResource()) {
            Pipeline pipe = jedis.pipelined();
            for (TaskId task : taskList) {
                responses.add(pipe.hgetAll(taskKey(task)));
            }
            pipe.sync();
        }
        Map<TaskId, TaskState> result = new HashMap<>();
        for (int i = 0; i < taskList.size(); i++) {
            Map<String, String> hash = responses.get(i).get();
            if (hash != null && !hash.isEmpty()) {
                result.put(taskList.get(i), TaskStateSerde.mapToTaskState(hash));
            }
        }
        return result;
    }

    @Override
    public void saveTaskState(TaskId task, TaskState state) {
        String key = taskKey(task);
        Map<String, String> hash = TaskStateSerde.taskStateToMap(state);
        try (var jedis = jedisPool.getResource()) {
            // Write all fields. Using HSET (not DEL+HSET) avoids a transient window
            // where the key is absent between the delete and the write.
            jedis.hset(key, hash);
            // Remove stale change_id_* fields when the new state has no lastConsumedChangeId
            // (i.e. only window boundaries are present, no partially-consumed change yet).
            if (!hash.containsKey(TaskStateSerde.TASK_STATE_CHANGE_ID_STREAM)) {
                jedis.hdel(key,
                        TaskStateSerde.TASK_STATE_CHANGE_ID_STREAM,
                        TaskStateSerde.TASK_STATE_CHANGE_ID_TIME);
            }
        }
    }

    @Override
    public void deleteTaskStates(Set<TaskId> tasks) {
        if (tasks.isEmpty()) {
            return;
        }
        String[] keys = tasks.stream().map(this::taskKey).toArray(String[]::new);
        try (var jedis = jedisPool.getResource()) {
            jedis.del(keys);
        }
    }

    // -------------------------------------------------------------------------
    // Generation ID operations (vnode-based)
    // -------------------------------------------------------------------------

    @Override
    public Optional<GenerationId> loadGenerationId() {
        try (var jedis = jedisPool.getResource()) {
            String value = jedis.get(globalGenKey());
            return value == null ? Optional.empty()
                    : Optional.of(TaskStateSerde.stringToGenerationId(value));
        }
    }

    @Override
    public void saveGenerationId(GenerationId generationId) {
        try (var jedis = jedisPool.getResource()) {
            jedis.set(globalGenKey(), TaskStateSerde.generationIdToString(generationId));
        }
    }

    // -------------------------------------------------------------------------
    // Generation ID operations (tablet-based, per-table)
    // -------------------------------------------------------------------------

    @Override
    public Optional<GenerationId> loadGenerationId(TableName table) {
        try (var jedis = jedisPool.getResource()) {
            String value = jedis.get(tableGenKey(table));
            return value == null ? Optional.empty()
                    : Optional.of(TaskStateSerde.stringToGenerationId(value));
        }
    }

    @Override
    public void saveGenerationId(TableName table, GenerationId generationId) {
        try (var jedis = jedisPool.getResource()) {
            jedis.set(tableGenKey(table), TaskStateSerde.generationIdToString(generationId));
        }
    }

    // -------------------------------------------------------------------------
    // Key helpers
    // -------------------------------------------------------------------------

    private String taskKey(TaskId task) {
        return keyPrefix + ":" + TASK_KEY_SEGMENT + ":" + TaskStateSerde.taskIdToKey(task);
    }

    private String globalGenKey() {
        return keyPrefix + ":" + GEN_KEY_SEGMENT;
    }

    private String tableGenKey(TableName table) {
        return keyPrefix + ":" + GEN_KEY_SEGMENT + ":" + table.keyspace + "." + table.name;
    }
}
