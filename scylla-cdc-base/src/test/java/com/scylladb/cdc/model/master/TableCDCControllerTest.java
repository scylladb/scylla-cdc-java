package com.scylladb.cdc.model.master;

import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.transport.GroupedTasks;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Iterator;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TableCDCControllerTest {

    @Test
    public void createsOneDeterministicTaskPerTabletStream() {
        SortedSet<StreamId> streams = new TreeSet<>();
        streams.add(tabletStream(30));
        streams.add(tabletStream(10));
        streams.add(tabletStream(20));
        GenerationMetadata generation = new GenerationMetadata(
                new Timestamp(new Date(1_700_000_000_000L)), Optional.empty(), streams);
        TableName table = new TableName("ks", "tablets_table");

        GroupedTasks tasks = TableCDCController.createTasks(generation, table);
        GroupedTasks recreatedTasks = TableCDCController.createTasks(generation, table);

        assertEquals(streams.size(), tasks.size());
        assertEquals(tasks.getTasks(), recreatedTasks.getTasks());
        tasks.getTasks().values().forEach(taskStreams -> assertEquals(1, taskStreams.size()));

        Iterator<StreamId> sortedStreams = streams.iterator();
        Iterator<TaskId> sortedTasks = tasks.getTaskIds().stream().sorted().iterator();
        for (int taskIndex = 0; taskIndex < streams.size(); taskIndex++) {
            TaskId taskId = sortedTasks.next();
            assertEquals(TaskId.forTabletStream(generation.getId(), taskIndex, table), taskId);
            assertEquals(sortedStreams.next(), tasks.getStreamsForTask(taskId).first());
        }
    }

    private static StreamId tabletStream(long token) {
        ByteBuffer value = ByteBuffer.allocate(16);
        value.putLong(token);
        value.putLong(1L); // version 1 with vnode index bits 4..25 set to zero
        value.flip();
        return new StreamId(value);
    }
}
