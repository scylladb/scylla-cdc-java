package com.scylladb.cdc.transport;

import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.master.GenerationMetadata;
import com.scylladb.cdc.model.master.MockGenerationMetadata;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;

import static org.junit.jupiter.api.Assertions.*;

public class GroupedTasksTest {
    private static final Timestamp GENERATION_START = new Timestamp(new Date(5 * 60 * 1000));
    private static final GenerationMetadata TEST_GENERATION = MockGenerationMetadata.mockGenerationMetadata(
            GENERATION_START, Optional.empty(), 4, 2);
    private static final TableName TEST_TABLE = new TableName("ks", "t");

    @Test
    public void testConstructWithGenerationMetadata() {
        GroupedTasks tasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                TEST_GENERATION, Collections.singleton(TEST_TABLE));

        assertEquals(TEST_GENERATION.getId(), tasks.getGenerationId());
        assertEquals(TEST_GENERATION, tasks.getGenerationMetadata());
        assertFalse(tasks.getTasks().isEmpty());
    }

    @Test
    public void testConstructWithGenerationIdOnly() {
        // Simulate what a distributed transport would do: reconstruct GroupedTasks
        // from serialized TaskId -> StreamId mapping + GenerationId, without
        // needing to re-fetch GenerationMetadata from the database.
        GroupedTasks fullTasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                TEST_GENERATION, Collections.singleton(TEST_TABLE));

        GenerationId generationId = fullTasks.getGenerationId();
        Map<TaskId, SortedSet<StreamId>> taskMap = fullTasks.getTasks();

        // Reconstruct with just GenerationId
        GroupedTasks reconstructed = new GroupedTasks(taskMap, generationId);

        assertEquals(generationId, reconstructed.getGenerationId());
        assertNull(reconstructed.getGenerationMetadata());
        assertEquals(taskMap, reconstructed.getTasks());
        assertEquals(fullTasks.size(), reconstructed.size());
    }

    @Test
    public void testGenerationIdMismatchThrows() {
        GenerationId wrongId = new GenerationId(new Timestamp(new Date(999)));
        Map<TaskId, SortedSet<StreamId>> taskMap = MockGenerationMetadata.generationMetadataToWorkerTasks(
                TEST_GENERATION, Collections.singleton(TEST_TABLE)).getTasks();

        assertThrows(IllegalArgumentException.class,
                () -> new GroupedTasks(taskMap, wrongId));
    }

    @Test
    public void testEmptyTasksWithGenerationId() {
        GenerationId generationId = TEST_GENERATION.getId();
        Map<TaskId, SortedSet<StreamId>> emptyTasks = new HashMap<>();

        GroupedTasks tasks = new GroupedTasks(emptyTasks, generationId);

        assertEquals(0, tasks.size());
        assertEquals(generationId, tasks.getGenerationId());
        assertNull(tasks.getGenerationMetadata());
    }

    @Test
    public void testGetStreamsForTask() {
        GroupedTasks tasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                TEST_GENERATION, Collections.singleton(TEST_TABLE));

        TaskId firstTask = tasks.getTaskIds().iterator().next();
        SortedSet<StreamId> streams = tasks.getStreamsForTask(firstTask);

        assertNotNull(streams);
        assertFalse(streams.isEmpty());
    }
}
