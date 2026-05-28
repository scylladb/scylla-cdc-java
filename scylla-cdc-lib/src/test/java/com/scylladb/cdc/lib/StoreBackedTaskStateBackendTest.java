package com.scylladb.cdc.lib;

import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.VNodeId;
import com.scylladb.cdc.model.worker.TaskState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link StoreBackedTaskStateBackend}.
 *
 * <p>Verifies that all state operations delegate correctly to the underlying {@link CDCStateStore},
 * that the in-process {@code activeTasks} set is updated correctly, and that {@link #replaceState}
 * detects inactive tasks without requiring a store read.
 */
class StoreBackedTaskStateBackendTest {

    /** Minimal recording CDCStateStore for verification. */
    static class SpyStore implements CDCStateStore {
        final Map<TaskId, TaskState> states = new ConcurrentHashMap<>();
        int saveCount = 0;
        int deleteCount = 0;
        int saveGenCount = 0;
        GenerationId savedGenerationId = null;
        final Map<TableName, GenerationId> savedTableGenerationIds = new HashMap<>();

        @Override
        public Map<TaskId, TaskState> loadTaskStates(Set<TaskId> tasks) {
            Map<TaskId, TaskState> result = new java.util.HashMap<>();
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

        @Override public Optional<GenerationId> loadGenerationId() {
            return savedGenerationId == null ? Optional.empty() : Optional.of(savedGenerationId);
        }
        @Override public void saveGenerationId(GenerationId id) { saveGenCount++; savedGenerationId = id; }
        @Override public Optional<GenerationId> loadGenerationId(TableName table) {
            return Optional.ofNullable(savedTableGenerationIds.get(table));
        }
        @Override public void saveGenerationId(TableName table, GenerationId id) { saveGenCount++; savedTableGenerationIds.put(table, id); }
    }

    private SpyStore store;
    private StoreBackedTaskStateBackend backend;
    private TaskId taskId;
    private TaskState taskState;
    private TaskState updatedState;

    @BeforeEach
    void setUp() {
        store = new SpyStore();
        backend = new StoreBackedTaskStateBackend(store);
        GenerationId gen = new GenerationId(new Timestamp(new Date(1_700_000_000_000L)));
        taskId = new TaskId(gen, new VNodeId(0), new TableName("ks", "tbl"));
        Timestamp start = new Timestamp(new Date(1_000L));
        Timestamp end = new Timestamp(new Date(2_000L));
        taskState = new TaskState(start, end, Optional.empty());
        updatedState = new TaskState(start, new Timestamp(new Date(3_000L)), Optional.empty());
    }

    @Test
    void constructor_nullStore_throws() {
        assertThrows(NullPointerException.class, () -> new StoreBackedTaskStateBackend(null));
    }

    @Test
    void setState_addsToActiveSetAndDelegatesToStore() {
        backend.setState(taskId, taskState);
        assertTrue(backend.getActiveTasks().contains(taskId));
        assertEquals(1, store.saveCount);
        assertTrue(store.states.containsKey(taskId));
    }

    @Test
    void getTaskStates_delegatesToStore() {
        store.states.put(taskId, taskState);
        Map<TaskId, TaskState> result = backend.getTaskStates(Set.of(taskId));
        assertEquals(1, result.size());
        assertSame(taskState, result.get(taskId));
    }

    @Test
    void replaceState_activeTask_delegatesToStoreAndReturnsTrue() {
        backend.setState(taskId, taskState);
        boolean replaced = backend.replaceState(taskId, updatedState);
        assertTrue(replaced);
        assertEquals(2, store.saveCount);
        assertSame(updatedState, store.states.get(taskId));
    }

    @Test
    void replaceState_inactiveTask_returnsFalseWithoutStoreWrite() {
        boolean replaced = backend.replaceState(taskId, updatedState);
        assertFalse(replaced);
        assertEquals(0, store.saveCount);
    }

    @Test
    void deleteTasks_removesFromActiveSetAndDelegatesToStore() {
        backend.setState(taskId, taskState);
        backend.deleteTasks(Set.of(taskId));
        assertFalse(backend.getActiveTasks().contains(taskId));
        assertEquals(1, store.deleteCount);
        assertFalse(store.states.containsKey(taskId));
    }

    @Test
    void deleteTasks_emptySet_skipsStoreCall() {
        backend.setState(taskId, taskState);
        backend.deleteTasks(Set.of());
        assertEquals(0, store.deleteCount);
        assertTrue(backend.getActiveTasks().contains(taskId));
    }

    @Test
    void saveGenerationId_vnode_delegatesToStore() {
        GenerationId gen = new GenerationId(new Timestamp(new Date(1_700_000_000_000L)));
        backend.saveGenerationId(gen);
        assertEquals(1, store.saveGenCount);
    }

    @Test
    void saveGenerationId_table_delegatesToStore() {
        GenerationId gen = new GenerationId(new Timestamp(new Date(1_700_000_000_000L)));
        TableName table = new TableName("ks", "tbl");
        backend.saveGenerationId(table, gen);
        assertEquals(1, store.saveGenCount);
    }

    @Test
    void getActiveTasks_returnsUnmodifiableView() {
        backend.setState(taskId, taskState);
        Set<TaskId> active = backend.getActiveTasks();
        assertThrows(UnsupportedOperationException.class, () -> active.remove(taskId));
    }

    @Test
    void loadGenerationId_vnode_delegatesToStore() {
        GenerationId gen = new GenerationId(new Timestamp(new Date(1_700_000_000_000L)));
        backend.saveGenerationId(gen);
        Optional<GenerationId> loaded = backend.loadGenerationId();
        assertTrue(loaded.isPresent());
        assertEquals(gen, loaded.get());
    }

    @Test
    void loadGenerationId_table_delegatesToStore() {
        GenerationId gen = new GenerationId(new Timestamp(new Date(1_700_000_000_000L)));
        TableName table = new TableName("ks", "tbl");
        backend.saveGenerationId(table, gen);
        Optional<GenerationId> loaded = backend.loadGenerationId(table);
        assertTrue(loaded.isPresent());
        assertEquals(gen, loaded.get());
    }

    @Test
    void loadGenerationId_vnode_emptyWhenNotSaved() {
        assertFalse(backend.loadGenerationId().isPresent());
    }

    @Test
    void loadGenerationId_table_emptyWhenNotSaved() {
        assertFalse(backend.loadGenerationId(new TableName("ks", "tbl")).isPresent());
    }
}
