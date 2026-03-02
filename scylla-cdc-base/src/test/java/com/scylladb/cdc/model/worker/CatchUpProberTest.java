package com.scylladb.cdc.model.worker;

import com.scylladb.cdc.cql.MockWorkerCQL;
import com.scylladb.cdc.model.*;
import com.scylladb.cdc.model.master.GenerationMetadata;
import com.scylladb.cdc.model.master.MockGenerationMetadata;
import com.scylladb.cdc.transport.GroupedTasks;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Direct unit tests for {@link CatchUpProber} in isolation, without going
 * through the full Worker/WorkerThread stack.
 */
class CatchUpProberTest {

    private static final TableName TEST_TABLE = new TableName("ks", "tbl");
    private static final long WINDOW_SIZE_MS = 30_000;
    private static final long PROBE_TIMEOUT_SECONDS = 5;

    private static GenerationMetadata closedGeneration(long startMs, long endMs, int vnodes, int streamsPerVnode) {
        return MockGenerationMetadata.mockGenerationMetadata(
                new Timestamp(new Date(startMs)), Optional.of(new Timestamp(new Date(endMs))),
                vnodes, streamsPerVnode);
    }

    private static GroupedTasks toGroupedTasks(GenerationMetadata gen) {
        return MockGenerationMetadata.generationMetadataToWorkerTasks(gen, Collections.singleton(TEST_TABLE));
    }

    private static List<Task> buildTasks(GroupedTasks grouped, long windowSizeMs) {
        TaskState initialState = TaskState.createInitialFor(grouped.getGenerationId(), windowSizeMs);
        return grouped.getTasks().entrySet().stream()
                .map(e -> new Task(e.getKey(), e.getValue(), initialState))
                .collect(Collectors.toList());
    }

    @Test
    void testApplySkipsWhenCutoffEmpty() {
        GenerationMetadata gen = closedGeneration(300_000, 600_000, 1, 1);
        GroupedTasks grouped = toGroupedTasks(gen);
        List<Task> tasks = buildTasks(grouped, WINDOW_SIZE_MS);

        MockWorkerCQL cql = new MockWorkerCQL();
        CatchUpProber prober = new CatchUpProber(cql, WINDOW_SIZE_MS, PROBE_TIMEOUT_SECONDS);
        List<Task> result = prober.apply(tasks, Collections.emptyMap(), grouped, Optional.empty());

        assertSame(tasks, result, "Should return same list when cutoff is empty");
        assertEquals(0, cql.getProbeInvocationCount());
    }

    @Test
    void testApplySkipsOpenGeneration() {
        GenerationMetadata gen = MockGenerationMetadata.mockGenerationMetadata(
                new Timestamp(new Date(300_000)), Optional.empty(), 1, 1);
        GroupedTasks grouped = toGroupedTasks(gen);
        List<Task> tasks = buildTasks(grouped, WINDOW_SIZE_MS);

        MockWorkerCQL cql = new MockWorkerCQL();
        CatchUpProber prober = new CatchUpProber(cql, WINDOW_SIZE_MS, PROBE_TIMEOUT_SECONDS);
        List<Task> result = prober.apply(tasks, Collections.emptyMap(), grouped,
                Optional.of(new Date(200_000)));

        assertSame(tasks, result, "Should return same list for open generation");
        assertEquals(0, cql.getProbeInvocationCount());
    }

    @Test
    void testApplySkipsTasksWithSavedState() {
        GenerationMetadata gen = closedGeneration(300_000, 600_000, 1, 1);
        GroupedTasks grouped = toGroupedTasks(gen);
        List<Task> tasks = buildTasks(grouped, WINDOW_SIZE_MS);

        // All tasks have saved state
        Map<TaskId, TaskState> states = new HashMap<>();
        for (Task t : tasks) {
            states.put(t.id, t.state);
        }

        MockWorkerCQL cql = new MockWorkerCQL();
        CatchUpProber prober = new CatchUpProber(cql, WINDOW_SIZE_MS, PROBE_TIMEOUT_SECONDS);
        // Cutoff far in future so window start is "old"
        List<Task> result = prober.apply(tasks, states, grouped, Optional.of(new Date(Long.MAX_VALUE / 2)));

        assertSame(tasks, result, "Should return same list when all tasks have saved state");
        assertEquals(0, cql.getProbeInvocationCount());
    }

    @Test
    void testApplyAdvancesTaskWhenProbeFindsData() {
        long genStart = 300_000;
        long genEnd = 600_000;
        GenerationMetadata gen = closedGeneration(genStart, genEnd, 1, 1);
        GroupedTasks grouped = toGroupedTasks(gen);
        List<Task> tasks = buildTasks(grouped, WINDOW_SIZE_MS);

        // Set up a probe result at 400_000ms
        long probeResultMs = 400_000;
        MockWorkerCQL cql = new MockWorkerCQL();
        cql.setProbeResultOverride(Optional.of(new Timestamp(new Date(probeResultMs))));

        CatchUpProber prober = new CatchUpProber(cql, WINDOW_SIZE_MS, PROBE_TIMEOUT_SECONDS);
        // Cutoff after genStart so task qualifies
        List<Task> result = prober.apply(tasks, Collections.emptyMap(), grouped,
                Optional.of(new Date(genEnd + 100_000)));

        assertEquals(tasks.size(), result.size());
        assertTrue(cql.getProbeInvocationCount() > 0);

        // The task should be advanced to the probe result
        Task advanced = result.get(0);
        assertEquals(probeResultMs, advanced.state.getWindowStartTimestamp().toDate().getTime());
    }

    @Test
    void testApplyDoesNotAdvanceWhenProbeReturnsEmpty() {
        long genStart = 300_000;
        long genEnd = 600_000;
        GenerationMetadata gen = closedGeneration(genStart, genEnd, 1, 1);
        GroupedTasks grouped = toGroupedTasks(gen);
        List<Task> tasks = buildTasks(grouped, WINDOW_SIZE_MS);

        MockWorkerCQL cql = new MockWorkerCQL();
        cql.setProbeResultOverride(Optional.empty());

        CatchUpProber prober = new CatchUpProber(cql, WINDOW_SIZE_MS, PROBE_TIMEOUT_SECONDS);
        List<Task> result = prober.apply(tasks, Collections.emptyMap(), grouped,
                Optional.of(new Date(genEnd + 100_000)));

        // Task should not be advanced
        Task original = tasks.get(0);
        Task resultTask = result.get(0);
        assertEquals(original.state.getWindowStartTimestamp(), resultTask.state.getWindowStartTimestamp());
    }

    @Test
    void testApplyHandlesProbeFailureGracefully() {
        long genStart = 300_000;
        long genEnd = 600_000;
        GenerationMetadata gen = closedGeneration(genStart, genEnd, 1, 1);
        GroupedTasks grouped = toGroupedTasks(gen);
        List<Task> tasks = buildTasks(grouped, WINDOW_SIZE_MS);

        MockWorkerCQL cql = new MockWorkerCQL();
        cql.setProbeFailureEnabled(true);

        CatchUpProber prober = new CatchUpProber(cql, WINDOW_SIZE_MS, PROBE_TIMEOUT_SECONDS);
        List<Task> result = prober.apply(tasks, Collections.emptyMap(), grouped,
                Optional.of(new Date(genEnd + 100_000)));

        // Should fall back to original window
        Task resultTask = result.get(0);
        assertEquals(genStart, resultTask.state.getWindowStartTimestamp().toDate().getTime());
    }

    @Test
    void testApplyMixedCandidatesAndSavedState() {
        long genStart = 300_000;
        long genEnd = 600_000;
        // 2 vnodes, each with 1 stream
        GenerationMetadata gen = closedGeneration(genStart, genEnd, 2, 1);
        GroupedTasks grouped = toGroupedTasks(gen);
        List<Task> tasks = buildTasks(grouped, WINDOW_SIZE_MS);

        // Save state for first task only
        Map<TaskId, TaskState> states = new HashMap<>();
        states.put(tasks.get(0).id, tasks.get(0).state);

        long probeResultMs = 450_000;
        MockWorkerCQL cql = new MockWorkerCQL();
        cql.setProbeResultOverride(Optional.of(new Timestamp(new Date(probeResultMs))));

        CatchUpProber prober = new CatchUpProber(cql, WINDOW_SIZE_MS, PROBE_TIMEOUT_SECONDS);
        List<Task> result = prober.apply(tasks, states, grouped,
                Optional.of(new Date(genEnd + 100_000)));

        assertEquals(2, result.size());
        // First task (with saved state) should be unchanged
        assertEquals(tasks.get(0).state.getWindowStartTimestamp(),
                result.get(0).state.getWindowStartTimestamp());
        // Second task (without saved state) should be advanced
        assertEquals(probeResultMs, result.get(1).state.getWindowStartTimestamp().toDate().getTime());
        // Only 1 probe call (for the unsaved task)
        assertEquals(1, cql.getProbeInvocationCount());
    }

    @Test
    void testApplyClampsToGenerationEnd() {
        long genStart = 300_000;
        long genEnd = 600_000;
        GenerationMetadata gen = closedGeneration(genStart, genEnd, 1, 1);
        GroupedTasks grouped = toGroupedTasks(gen);
        List<Task> tasks = buildTasks(grouped, WINDOW_SIZE_MS);

        // Probe result is beyond generation end
        MockWorkerCQL cql = new MockWorkerCQL();
        cql.setProbeResultOverride(Optional.of(new Timestamp(new Date(genEnd + 50_000))));

        CatchUpProber prober = new CatchUpProber(cql, WINDOW_SIZE_MS, PROBE_TIMEOUT_SECONDS);
        List<Task> result = prober.apply(tasks, Collections.emptyMap(), grouped,
                Optional.of(new Date(genEnd + 100_000)));

        // Should be clamped to genEnd - 1ms
        Task advanced = result.get(0);
        assertEquals(genEnd - 1, advanced.state.getWindowStartTimestamp().toDate().getTime());
    }

    @Test
    void testTaskStateCreateForWindowEpochZero() {
        Timestamp start = new Timestamp(new Date(0));
        TaskState state = TaskState.createForWindow(start, WINDOW_SIZE_MS);
        assertEquals(0, state.getWindowStartTimestamp().toDate().getTime());
        assertEquals(WINDOW_SIZE_MS, state.getWindowEndTimestamp().toDate().getTime());
    }

    @Test
    void testTaskStateCreateForWindowLargeTimestamp() {
        // Use a large but realistic timestamp (year ~2100)
        long largeMs = 4_102_444_800_000L; // 2100-01-01
        Timestamp start = new Timestamp(new Date(largeMs));
        TaskState state = TaskState.createForWindow(start, WINDOW_SIZE_MS);
        assertEquals(largeMs, state.getWindowStartTimestamp().toDate().getTime());
        assertEquals(largeMs + WINDOW_SIZE_MS, state.getWindowEndTimestamp().toDate().getTime());
    }

    @Test
    void testApplySkipsRecentWindows() {
        long genStart = 300_000;
        long genEnd = 600_000;
        GenerationMetadata gen = closedGeneration(genStart, genEnd, 1, 1);
        GroupedTasks grouped = toGroupedTasks(gen);
        List<Task> tasks = buildTasks(grouped, WINDOW_SIZE_MS);

        MockWorkerCQL cql = new MockWorkerCQL();
        CatchUpProber prober = new CatchUpProber(cql, WINDOW_SIZE_MS, PROBE_TIMEOUT_SECONDS);
        // Cutoff is BEFORE the window start, so the task's window is already recent enough
        List<Task> result = prober.apply(tasks, Collections.emptyMap(), grouped,
                Optional.of(new Date(genStart - 1)));

        assertSame(tasks, result, "Should skip probing when window is already recent");
        assertEquals(0, cql.getProbeInvocationCount());
    }
}
