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
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link InProcessTaskStateBackend}.
 *
 * <p>Verifies the original {@link java.util.concurrent.ConcurrentHashMap}-based behaviour:
 * task states are held in-process, {@link #replaceState} detects inactive tasks, and
 * {@link #saveGenerationId} is a no-op (no exception thrown, no side effects).
 */
class InProcessTaskStateBackendTest {

    private InProcessTaskStateBackend backend;
    private TaskId taskId;
    private TaskState taskState;
    private TaskState updatedState;

    @BeforeEach
    void setUp() {
        backend = new InProcessTaskStateBackend();
        GenerationId gen = new GenerationId(new Timestamp(new Date(1_700_000_000_000L)));
        taskId = new TaskId(gen, new VNodeId(0), new TableName("ks", "tbl"));
        Timestamp start = new Timestamp(new Date(1_000L));
        Timestamp end = new Timestamp(new Date(2_000L));
        taskState = new TaskState(start, end, Optional.empty());
        updatedState = new TaskState(start, new Timestamp(new Date(3_000L)), Optional.empty());
    }

    @Test
    void setState_makesTaskActiveAndSavesState() {
        backend.setState(taskId, taskState);
        assertTrue(backend.getActiveTasks().contains(taskId));
        Map<TaskId, TaskState> loaded = backend.getTaskStates(Set.of(taskId));
        assertEquals(1, loaded.size());
        assertSame(taskState, loaded.get(taskId));
    }

    @Test
    void getTaskStates_unknownTaskAbsent() {
        Map<TaskId, TaskState> result = backend.getTaskStates(Set.of(taskId));
        assertTrue(result.isEmpty());
    }

    @Test
    void replaceState_activeTask_updatesAndReturnsTrue() {
        backend.setState(taskId, taskState);
        boolean replaced = backend.replaceState(taskId, updatedState);
        assertTrue(replaced);
        assertSame(updatedState, backend.getTaskStates(Set.of(taskId)).get(taskId));
    }

    @Test
    void replaceState_inactiveTask_returnsFalse() {
        boolean replaced = backend.replaceState(taskId, updatedState);
        assertFalse(replaced);
        // State must NOT be written
        assertTrue(backend.getTaskStates(Set.of(taskId)).isEmpty());
    }

    @Test
    void deleteTasks_removesFromActiveSetAndStore() {
        backend.setState(taskId, taskState);
        backend.deleteTasks(Set.of(taskId));
        assertFalse(backend.getActiveTasks().contains(taskId));
        assertTrue(backend.getTaskStates(Set.of(taskId)).isEmpty());
    }

    @Test
    void deleteTasks_emptySet_noOp() {
        backend.setState(taskId, taskState);
        backend.deleteTasks(Set.of());
        assertTrue(backend.getActiveTasks().contains(taskId));
    }

    @Test
    void areTasksFullyConsumedUntil_allPassed_returnsTrue() {
        // taskState has window end = 2_000L; until = 1_500L — hasPassed depends on TaskState logic
        backend.setState(taskId, taskState);
        Timestamp before = new Timestamp(new Date(500L));
        // The task has progressed past 500L (start=1000 > 500)
        assertTrue(backend.areTasksFullyConsumedUntil(Set.of(taskId), before));
    }

    @Test
    void areTasksFullyConsumedUntil_notPassed_returnsFalse() {
        backend.setState(taskId, taskState);
        Timestamp after = new Timestamp(new Date(5_000L));
        assertFalse(backend.areTasksFullyConsumedUntil(Set.of(taskId), after));
    }

    @Test
    void areTasksFullyConsumedUntil_unknownTask_returnsFalse() {
        assertFalse(backend.areTasksFullyConsumedUntil(Set.of(taskId), new Timestamp(new Date(0L))));
    }

    @Test
    void saveGenerationId_vnode_noOp() {
        // Must not throw
        GenerationId gen = new GenerationId(new Timestamp(new Date(1_700_000_000_000L)));
        assertDoesNotThrow(() -> backend.saveGenerationId(gen));
    }

    @Test
    void saveGenerationId_table_noOp() {
        GenerationId gen = new GenerationId(new Timestamp(new Date(1_700_000_000_000L)));
        TableName table = new TableName("ks", "tbl");
        assertDoesNotThrow(() -> backend.saveGenerationId(table, gen));
    }

    @Test
    void loadGenerationId_vnode_alwaysEmpty() {
        // In-process backend never persists generation IDs — preserves original master behavior
        GenerationId gen = new GenerationId(new Timestamp(new Date(1_700_000_000_000L)));
        backend.saveGenerationId(gen);
        assertFalse(backend.loadGenerationId().isPresent());
    }

    @Test
    void loadGenerationId_table_alwaysEmpty() {
        GenerationId gen = new GenerationId(new Timestamp(new Date(1_700_000_000_000L)));
        TableName table = new TableName("ks", "tbl");
        backend.saveGenerationId(table, gen);
        assertFalse(backend.loadGenerationId(table).isPresent());
    }
}
