package com.scylladb.cdc.lib;

import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.VNodeId;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeTime;
import com.scylladb.cdc.model.worker.TaskState;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class TaskStateSerdeTest {

    private static TaskId makeTaskId(long genMs, int vnode) {
        GenerationId gen = new GenerationId(new Timestamp(new Date(genMs)));
        return new TaskId(gen, new VNodeId(vnode), new TableName("my_ks", "my_table"));
    }

    @Test
    void taskIdKey_roundTrip() {
        TaskId taskId = makeTaskId(1_700_000_000_000L, 42);
        String key = TaskStateSerde.taskIdToKey(taskId);
        assertEquals("my_ks.my_table:1700000000000:42", key);

        TaskId restored = TaskStateSerde.keyToTaskId(key);
        assertEquals(taskId.getGenerationId().getGenerationStart().toDate().getTime(),
                restored.getGenerationId().getGenerationStart().toDate().getTime());
        assertEquals(taskId.getvNodeId().getIndex(), restored.getvNodeId().getIndex());
        assertEquals(taskId.getTable().keyspace, restored.getTable().keyspace);
        assertEquals(taskId.getTable().name, restored.getTable().name);
    }

    @Test
    void taskIdKey_invalidFormat_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> TaskStateSerde.keyToTaskId("bad_format"));
    }

    @Test
    void taskStateMap_roundTrip_noChangeId() {
        Timestamp start = new Timestamp(new Date(1_000L));
        Timestamp end = new Timestamp(new Date(2_000L));
        TaskState state = new TaskState(start, end, Optional.empty());

        Map<String, String> map = TaskStateSerde.taskStateToMap(state);
        assertEquals("1000", map.get(TaskStateSerde.TASK_STATE_WINDOW_START));
        assertEquals("2000", map.get(TaskStateSerde.TASK_STATE_WINDOW_END));
        assertFalse(map.containsKey(TaskStateSerde.TASK_STATE_CHANGE_ID_STREAM));
        assertFalse(map.containsKey(TaskStateSerde.TASK_STATE_CHANGE_ID_TIME));

        TaskState restored = TaskStateSerde.mapToTaskState(map);
        assertEquals(1_000L, restored.getWindowStartTimestamp().toDate().getTime());
        assertEquals(2_000L, restored.getWindowEndTimestamp().toDate().getTime());
        assertFalse(restored.getLastConsumedChangeId().isPresent());
    }

    @Test
    void taskStateMap_roundTrip_withChangeId() {
        Timestamp start = new Timestamp(new Date(1_000L));
        Timestamp end = new Timestamp(new Date(2_000L));
        byte[] streamBytes = new byte[16];
        streamBytes[0] = (byte) 0xAB;
        StreamId streamId = new StreamId(ByteBuffer.wrap(streamBytes));
        UUID uuid = UUID.randomUUID();
        ChangeTime changeTime = new ChangeTime(uuid);
        ChangeId changeId = new ChangeId(streamId, changeTime);
        TaskState state = new TaskState(start, end, Optional.of(changeId));

        Map<String, String> map = TaskStateSerde.taskStateToMap(state);
        assertTrue(map.containsKey(TaskStateSerde.TASK_STATE_CHANGE_ID_STREAM));
        assertTrue(map.containsKey(TaskStateSerde.TASK_STATE_CHANGE_ID_TIME));
        assertEquals(uuid.toString(), map.get(TaskStateSerde.TASK_STATE_CHANGE_ID_TIME));

        TaskState restored = TaskStateSerde.mapToTaskState(map);
        assertTrue(restored.getLastConsumedChangeId().isPresent());
        assertEquals(uuid, restored.getLastConsumedChangeId().get().getChangeTime().getUUID());
    }

    @Test
    void generationId_roundTrip() {
        GenerationId gen = new GenerationId(new Timestamp(new Date(12345L)));
        String s = TaskStateSerde.generationIdToString(gen);
        assertEquals("12345", s);
        GenerationId restored = TaskStateSerde.stringToGenerationId(s);
        assertEquals(12345L, restored.getGenerationStart().toDate().getTime());
    }

    @Test
    void streamIdHex_roundTrip() {
        byte[] bytes = new byte[16];
        for (int i = 0; i < 16; i++) bytes[i] = (byte) i;
        StreamId streamId = new StreamId(ByteBuffer.wrap(bytes));
        String hex = TaskStateSerde.streamIdToHex(streamId);
        StreamId restored = TaskStateSerde.hexToStreamId(hex);
        // Compare bytes
        byte[] restoredBytes = new byte[16];
        restored.getValue().get(restoredBytes);
        assertArrayEquals(bytes, restoredBytes);
    }
}
