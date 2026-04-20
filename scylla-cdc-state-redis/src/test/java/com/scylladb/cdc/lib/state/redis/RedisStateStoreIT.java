package com.scylladb.cdc.lib.state.redis;

import com.scylladb.cdc.lib.TaskStateSerde;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.VNodeId;
import com.scylladb.cdc.model.worker.TaskState;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.JedisPool;

import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class RedisStateStoreIT {

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                    .withExposedPorts(6379);

    private static JedisPool jedisPool;
    private RedisStateStore store;
    private TaskId taskId;
    private TaskState taskState;

    @BeforeAll
    static void startPool() {
        jedisPool = new JedisPool(REDIS.getHost(), REDIS.getMappedPort(6379));
    }

    @AfterAll
    static void stopPool() {
        jedisPool.close();
    }

    @BeforeEach
    void setUp() {
        store = new RedisStateStore(jedisPool, "test");
        // Flush Redis before each test for isolation
        try (var jedis = jedisPool.getResource()) {
            jedis.flushAll();
        }

        GenerationId gen = new GenerationId(new Timestamp(new Date(1_700_000_000_000L)));
        taskId = new TaskId(gen, new VNodeId(0), new TableName("ks", "tbl"));
        Timestamp start = new Timestamp(new Date(1_700_000_000_000L));
        Timestamp end = new Timestamp(new Date(1_700_000_060_000L));
        taskState = new TaskState(start, end, Optional.empty());
    }

    @Test
    void loadTaskStates_returnsEmpty_whenNothingSaved() {
        assertTrue(store.loadTaskStates(Set.of(taskId)).isEmpty());
    }

    @Test
    void saveAndLoad_roundTrips() {
        store.saveTaskState(taskId, taskState);
        Map<TaskId, TaskState> result = store.loadTaskStates(Set.of(taskId));
        assertEquals(1, result.size());
        TaskState loaded = result.get(taskId);
        assertNotNull(loaded);
        assertEquals(taskState.getWindowStartTimestamp().toDate().getTime(),
                loaded.getWindowStartTimestamp().toDate().getTime());
        assertEquals(taskState.getWindowEndTimestamp().toDate().getTime(),
                loaded.getWindowEndTimestamp().toDate().getTime());
    }

    @Test
    void saveTaskState_overwritesPreviousState() {
        store.saveTaskState(taskId, taskState);
        Timestamp newEnd = new Timestamp(new Date(1_700_000_120_000L));
        TaskState updated = new TaskState(taskState.getWindowStartTimestamp(), newEnd, Optional.empty());
        store.saveTaskState(taskId, updated);

        Map<TaskId, TaskState> result = store.loadTaskStates(Set.of(taskId));
        assertEquals(1_700_000_120_000L, result.get(taskId).getWindowEndTimestamp().toDate().getTime());
    }

    @Test
    void deleteTaskStates_removesEntries() {
        store.saveTaskState(taskId, taskState);
        store.deleteTaskStates(Set.of(taskId));
        assertTrue(store.loadTaskStates(Set.of(taskId)).isEmpty());
    }

    @Test
    void globalGenerationId_saveAndLoad() {
        assertFalse(store.loadGenerationId().isPresent());
        GenerationId gen = new GenerationId(new Timestamp(new Date(42_000L)));
        store.saveGenerationId(gen);
        Optional<GenerationId> loaded = store.loadGenerationId();
        assertTrue(loaded.isPresent());
        assertEquals(42_000L, loaded.get().getGenerationStart().toDate().getTime());
    }

    @Test
    void tableGenerationId_saveAndLoad() {
        TableName table = new TableName("ks", "tbl");
        assertFalse(store.loadGenerationId(table).isPresent());
        GenerationId gen = new GenerationId(new Timestamp(new Date(99_000L)));
        store.saveGenerationId(table, gen);
        Optional<GenerationId> loaded = store.loadGenerationId(table);
        assertTrue(loaded.isPresent());
        assertEquals(99_000L, loaded.get().getGenerationStart().toDate().getTime());
    }

    @Test
    void customKeyPrefix_isolatesFromDefault() {
        RedisStateStore storeA = new RedisStateStore(jedisPool, "prefix-a");
        RedisStateStore storeB = new RedisStateStore(jedisPool, "prefix-b");
        storeA.saveTaskState(taskId, taskState);
        assertTrue(storeB.loadTaskStates(Set.of(taskId)).isEmpty(),
                "Stores with different prefixes should not share task state");
    }
}
