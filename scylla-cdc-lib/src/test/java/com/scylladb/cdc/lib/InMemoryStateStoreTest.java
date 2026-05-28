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

class InMemoryStateStoreTest {

    private InMemoryStateStore store;
    private TaskId taskId;
    private TaskState taskState;

    @BeforeEach
    void setUp() {
        store = new InMemoryStateStore();
        GenerationId gen = new GenerationId(new Timestamp(new Date(1_700_000_000_000L)));
        taskId = new TaskId(gen, new VNodeId(0), new TableName("ks", "tbl"));
        Timestamp start = new Timestamp(new Date(1_700_000_000_000L));
        Timestamp end = new Timestamp(new Date(1_700_000_060_000L));
        taskState = new TaskState(start, end, Optional.empty());
    }

    @Test
    void loadTaskStates_returnsEmpty_whenNothingSaved() {
        Map<TaskId, TaskState> result = store.loadTaskStates(Set.of(taskId));
        assertTrue(result.isEmpty());
    }

    @Test
    void saveAndLoad_roundTrips() {
        store.saveTaskState(taskId, taskState);
        Map<TaskId, TaskState> result = store.loadTaskStates(Set.of(taskId));
        assertEquals(1, result.size());
        TaskState loaded = result.get(taskId);
        assertNotNull(loaded);
        assertEquals(taskState.getWindowStartTimestamp(), loaded.getWindowStartTimestamp());
        assertEquals(taskState.getWindowEndTimestamp(), loaded.getWindowEndTimestamp());
    }

    @Test
    void deleteTaskStates_removesEntry() {
        store.saveTaskState(taskId, taskState);
        store.deleteTaskStates(Set.of(taskId));
        assertTrue(store.loadTaskStates(Set.of(taskId)).isEmpty());
    }

    @Test
    void generationId_saveAndLoad() {
        assertFalse(store.loadGenerationId().isPresent());
        GenerationId gen = new GenerationId(new Timestamp(new Date(42_000L)));
        store.saveGenerationId(gen);
        Optional<GenerationId> loaded = store.loadGenerationId();
        assertTrue(loaded.isPresent());
        assertEquals(gen.getGenerationStart().toDate().getTime(),
                loaded.get().getGenerationStart().toDate().getTime());
    }

    @Test
    void tableGenerationId_saveAndLoad() {
        TableName table = new TableName("ks", "tbl");
        assertFalse(store.loadGenerationId(table).isPresent());
        GenerationId gen = new GenerationId(new Timestamp(new Date(99_000L)));
        store.saveGenerationId(table, gen);
        Optional<GenerationId> loaded = store.loadGenerationId(table);
        assertTrue(loaded.isPresent());
        assertEquals(gen.getGenerationStart().toDate().getTime(),
                loaded.get().getGenerationStart().toDate().getTime());
    }

    @Test
    void areTasksFullyConsumedUntil_falseWhenNoState() {
        Timestamp until = new Timestamp(new Date(1_700_000_000_000L));
        assertFalse(store.areTasksFullyConsumedUntil(Set.of(taskId), until));
    }

    @Test
    void areTasksFullyConsumedUntil_trueWhenWindowEndPastUntil() {
        store.saveTaskState(taskId, taskState);
        // taskState window ends at 1_700_000_060_000L, so "until" before that => true
        Timestamp until = new Timestamp(new Date(1_699_999_999_000L));
        assertTrue(store.areTasksFullyConsumedUntil(Set.of(taskId), until));
    }

    @Test
    void areTasksFullyConsumedUntil_falseWhenWindowEndBeforeUntil() {
        store.saveTaskState(taskId, taskState);
        // taskState window ends at 1_700_000_060_000L, "until" after => not yet consumed
        Timestamp until = new Timestamp(new Date(1_700_000_120_000L));
        assertFalse(store.areTasksFullyConsumedUntil(Set.of(taskId), until));
    }
}
