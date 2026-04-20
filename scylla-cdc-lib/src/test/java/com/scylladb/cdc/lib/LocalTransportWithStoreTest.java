package com.scylladb.cdc.lib;

import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.VNodeId;
import com.scylladb.cdc.model.worker.TaskState;
import com.scylladb.cdc.transport.TaskAbortedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the state-delegation behaviour of {@link LocalTransport}.
 *
 * <p>Uses a simple in-process {@link CDCStateStore} spy to verify that
 * {@code setState}, {@code updateState}, {@code moveStateToNextWindow},
 * and {@code getTaskStates} all delegate correctly to the store, and that
 * {@link TaskAbortedException} is thrown for inactive tasks without requiring a
 * store read.
 */
class LocalTransportWithStoreTest {

    /** Minimal recording CDCStateStore for verification. */
    static class RecordingStateStore implements CDCStateStore {
        final Map<TaskId, TaskState> states = new ConcurrentHashMap<>();
        int saveCount = 0;
        int deleteCount = 0;

        @Override
        public Map<TaskId, TaskState> loadTaskStates(Set<TaskId> tasks) {
            Map<TaskId, TaskState> result = new HashMap<>();
            for (TaskId t : tasks) {
                if (states.containsKey(t)) result.put(t, states.get(t));
            }
            return result;
        }

        @Override
        public void saveTaskState(TaskId task, TaskState state) {
            saveCount++;
            states.put(task, state);
        }

        @Override
        public void deleteTaskStates(Set<TaskId> tasks) {
            deleteCount += tasks.size();
            tasks.forEach(states::remove);
        }

        @Override public Optional<GenerationId> loadGenerationId() { return Optional.empty(); }
        @Override public void saveGenerationId(GenerationId id) {}
        @Override public Optional<GenerationId> loadGenerationId(TableName table) { return Optional.empty(); }
        @Override public void saveGenerationId(TableName table, GenerationId id) {}
    }

    private RecordingStateStore store;
    private LocalTransport transport;
    private TaskId taskId;
    private TaskState taskState;

    @BeforeEach
    void setUp() {
        store = new RecordingStateStore();
        Supplier<ScheduledExecutorService> exec = () -> new ScheduledThreadPoolExecutor(1);
        // WorkerConfiguration.Builder requires a consumer — use a no-op one via InMemoryStateStore
        // We only test WorkerTransport state delegation here, not actual worker lifecycle.
        transport = new LocalTransport(
                new ThreadGroup("test"),
                com.scylladb.cdc.model.worker.WorkerConfiguration.builder()
                        .withConsumer(change -> java.util.concurrent.CompletableFuture.completedFuture(null)),
                exec,
                store);

        GenerationId gen = new GenerationId(new Timestamp(new Date(1_700_000_000_000L)));
        taskId = new TaskId(gen, new VNodeId(0), new TableName("ks", "tbl"));
        Timestamp start = new Timestamp(new Date(1_000L));
        Timestamp end = new Timestamp(new Date(2_000L));
        taskState = new TaskState(start, end, Optional.empty());
    }

    @Test
    void setState_delegatesToStore_andMarksActive() {
        transport.setState(taskId, taskState);
        assertEquals(1, store.saveCount);
        assertTrue(store.states.containsKey(taskId));
    }

    @Test
    void getTaskStates_delegatesToStore() {
        store.states.put(taskId, taskState);
        Map<TaskId, TaskState> result = transport.getTaskStates(Set.of(taskId));
        assertEquals(1, result.size());
        assertSame(taskState, result.get(taskId));
    }

    @Test
    void updateState_afterSetState_delegatesToStore() {
        transport.setState(taskId, taskState);
        Timestamp newEnd = new Timestamp(new Date(3_000L));
        TaskState updated = new TaskState(taskState.getWindowStartTimestamp(), newEnd, Optional.empty());
        transport.updateState(taskId, updated);
        assertEquals(2, store.saveCount);
        assertEquals(3_000L, store.states.get(taskId).getWindowEndTimestamp().toDate().getTime());
    }

    @Test
    void updateState_withoutSetState_throwsTaskAbortedException() {
        assertThrows(TaskAbortedException.class,
                () -> transport.updateState(taskId, taskState));
        // Store must NOT be called for the update
        assertEquals(0, store.saveCount);
    }

    @Test
    void moveStateToNextWindow_afterSetState_delegatesToStore() {
        transport.setState(taskId, taskState);
        Timestamp newEnd = new Timestamp(new Date(4_000L));
        TaskState next = new TaskState(taskState.getWindowStartTimestamp(), newEnd, Optional.empty());
        transport.moveStateToNextWindow(taskId, next);
        assertEquals(2, store.saveCount);
    }

    @Test
    void moveStateToNextWindow_withoutSetState_throwsTaskAbortedException() {
        assertThrows(TaskAbortedException.class,
                () -> transport.moveStateToNextWindow(taskId, taskState));
    }
}
