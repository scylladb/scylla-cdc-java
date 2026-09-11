package com.scylladb.cdc.model.worker;

import com.scylladb.cdc.cql.MockWorkerCQL;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.VNodeId;
import com.scylladb.cdc.transport.GroupedTasks;
import com.scylladb.cdc.transport.WorkerTransport;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.scylladb.cdc.model.worker.ChangeSchemaTest.TEST_SCHEMA_SIMPLE;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TabletTaskStateMigrationTest {

    private static final GenerationId GENERATION =
            new GenerationId(new Timestamp(new Date(1_700_000_000_000L)));
    private static final TableName TABLE = new TableName("ks", "tablets_table");

    @Test
    public void fansLegacyTabletCheckpointOutToEveryStreamTask() throws Exception {
        Map<TaskId, SortedSet<StreamId>> tasks = new HashMap<>();
        for (int index = 0; index < 3; index++) {
            tasks.put(tabletTaskId(index), singletonStream(tabletStream(index + 1)));
        }
        TaskState legacyState = new TaskState(
                new Timestamp(new Date(1_700_000_010_000L)),
                new Timestamp(new Date(1_700_000_020_000L)),
                Optional.empty());
        RecordingTransport transport = new RecordingTransport(
                Collections.emptyMap(), Collections.singletonMap(legacyTaskId(), legacyState));

        addTasksAndStop(workerWithTransport(transport), tasks);

        assertEquals(3, transport.persistedStates.size());
        tasks.keySet().forEach(task ->
                assertEquals(legacyState, transport.persistedStates.get(task)));
    }

    @Test
    public void nonEmptyLegacyCheckpointPreservesConsumedPrefix() throws Exception {
        StreamId earlierStream = tabletStream(1);
        StreamId checkpointStream = tabletStream(2);
        StreamId laterStream = tabletStream(3);
        Map<TaskId, SortedSet<StreamId>> tasks = new HashMap<>();
        tasks.put(tabletTaskId(0), singletonStream(earlierStream));
        tasks.put(tabletTaskId(1), singletonStream(checkpointStream));
        tasks.put(tabletTaskId(2), singletonStream(laterStream));

        long generationStartMs = GENERATION.getGenerationStart().toDate().getTime();
        MockRawChange earlierAfterCheckpointTime =
                rawChange(earlierStream, generationStartMs + 19_000);
        MockRawChange earlierAtNextWindowStart =
                rawChange(earlierStream, generationStartMs + 20_000);
        MockRawChange checkpointBefore =
                rawChange(checkpointStream, generationStartMs + 14_000);
        MockRawChange checkpoint =
                rawChange(checkpointStream, generationStartMs + 15_000);
        MockRawChange checkpointAfter =
                rawChange(checkpointStream, generationStartMs + 16_000);
        MockRawChange laterBeforeCheckpointTime =
                rawChange(laterStream, generationStartMs + 11_000);
        MockRawChange laterAfterCheckpointTime =
                rawChange(laterStream, generationStartMs + 19_000);

        TaskState legacyState = new TaskState(
                new Timestamp(new Date(generationStartMs + 10_000)),
                new Timestamp(new Date(generationStartMs + 20_000)),
                Optional.of(checkpoint.getId()));
        RecordingTransport transport = new RecordingTransport(
                Collections.emptyMap(), Collections.singletonMap(legacyTaskId(), legacyState));
        List<Task> tasksWithForeignCursors = new CopyOnWriteArrayList<>();
        MockWorkerCQL cql = new MockWorkerCQL() {
            @Override
            public CompletableFuture<WorkerCQL.Reader> createReader(Task task) {
                task.state.getLastConsumedChangeId().ifPresent(cursor -> {
                    if (!task.streams.contains(cursor.getStreamId())) {
                        tasksWithForeignCursors.add(task);
                    }
                });
                return super.createReader(task);
            }
        };
        cql.setRawChanges(List.of(earlierAfterCheckpointTime, earlierAtNextWindowStart,
                checkpointBefore, checkpoint, checkpointAfter, laterBeforeCheckpointTime,
                laterAfterCheckpointTime));
        List<RawChange> consumed = new CopyOnWriteArrayList<>();
        Worker worker = workerWithTransport(transport, cql,
                Consumer.syncRawChangeConsumer(consumed::add),
                Clock.fixed(Instant.ofEpochMilli(generationStartMs + 30_000), ZoneOffset.UTC));
        TaskState earlierState = legacyState.moveToNextWindow(10_000);
        TaskState laterState = new TaskState(legacyState.getWindowStartTimestamp(),
                legacyState.getWindowEndTimestamp(), Optional.empty());
        Map<TaskId, TaskState> expectedStates = Map.of(
                tabletTaskId(0), earlierState,
                tabletTaskId(1), legacyState,
                tabletTaskId(2), laterState);

        try {
            worker.addTasks(new GroupedTasks(tasks, GENERATION));
            await().atMost(3, TimeUnit.SECONDS).until(() -> tasks.entrySet().stream()
                    .allMatch(entry -> cql.isReaderFinished(new Task(
                            entry.getKey(), entry.getValue(), expectedStates.get(entry.getKey())))));
        } finally {
            worker.stop();
        }

        assertEquals(expectedStates, transport.persistedStates);
        assertTrue(tasksWithForeignCursors.isEmpty());
        assertEquals(List.of(earlierAtNextWindowStart), changesForStream(consumed, earlierStream));
        assertEquals(List.of(checkpointAfter), changesForStream(consumed, checkpointStream));
        assertEquals(List.of(laterBeforeCheckpointTime, laterAfterCheckpointTime),
                changesForStream(consumed, laterStream));
        assertEquals(4, consumed.size());
    }

    @Test
    public void normalizesLegacyCheckpointWhenCheckpointStreamIsAssignedElsewhere()
            throws Exception {
        StreamId earlierStream = tabletStream(1);
        StreamId checkpointStream = tabletStream(2);
        StreamId laterStream = tabletStream(3);
        Map<TaskId, SortedSet<StreamId>> tasks = new HashMap<>();
        tasks.put(tabletTaskId(0), singletonStream(earlierStream));
        tasks.put(tabletTaskId(2), singletonStream(laterStream));
        TaskState legacyState = new TaskState(
                new Timestamp(new Date(1_700_000_010_000L)),
                new Timestamp(new Date(1_700_000_020_000L)),
                Optional.of(rawChange(checkpointStream, 1_700_000_015_000L).getId()));
        RecordingTransport transport = new RecordingTransport(
                Collections.emptyMap(), Collections.singletonMap(legacyTaskId(), legacyState));

        addTasksAndStop(workerWithTransport(transport), tasks);

        assertEquals(legacyState.moveToNextWindow(10_000),
                transport.persistedStates.get(tabletTaskId(0)));
        assertEquals(new TaskState(legacyState.getWindowStartTimestamp(),
                        legacyState.getWindowEndTimestamp(), Optional.empty()),
                transport.persistedStates.get(tabletTaskId(2)));
    }

    @Test
    public void trimsExpiredLegacyCheckpointBeforeNormalizingPerStreamStates()
            throws Exception {
        StreamId earlierStream = tabletStream(1);
        StreamId checkpointStream = tabletStream(2);
        Map<TaskId, SortedSet<StreamId>> tasks = new HashMap<>();
        tasks.put(tabletTaskId(0), singletonStream(earlierStream));
        tasks.put(tabletTaskId(1), singletonStream(checkpointStream));
        long generationStartMs = GENERATION.getGenerationStart().toDate().getTime();
        TaskState legacyState = new TaskState(
                new Timestamp(new Date(generationStartMs)),
                new Timestamp(new Date(generationStartMs + 10_000)),
                Optional.of(rawChange(checkpointStream, generationStartMs + 5_000).getId()));
        RecordingTransport transport = new RecordingTransport(
                Collections.emptyMap(), Collections.singletonMap(legacyTaskId(), legacyState));
        MockWorkerCQL cql = new MockWorkerCQL();
        cql.setTablesTTL(Collections.singletonMap(TABLE, Optional.of(15L)));
        Worker worker = workerWithTransport(transport, cql,
                Consumer.syncRawChangeConsumer(change -> { }),
                Clock.fixed(Instant.ofEpochMilli(generationStartMs + 30_000), ZoneOffset.UTC));
        TaskState expectedState = new TaskState(
                new Timestamp(new Date(generationStartMs + 15_000)),
                new Timestamp(new Date(generationStartMs + 25_000)), Optional.empty());

        try {
            worker.addTasks(new GroupedTasks(tasks, GENERATION));
        } finally {
            worker.stop();
        }

        assertEquals(expectedState, transport.persistedStates.get(tabletTaskId(0)));
        assertEquals(expectedState, transport.persistedStates.get(tabletTaskId(1)));
    }

    @Test
    public void doesNotFanCheckpointOutForVnodeTasks() throws Exception {
        Map<TaskId, SortedSet<StreamId>> tasks = new HashMap<>();
        tasks.put(vnodeTaskId(0), singletonStream(vnodeStream(1, 0)));
        tasks.put(vnodeTaskId(1), singletonStream(vnodeStream(2, 1)));
        TaskState vnodeZeroState = new TaskState(
                new Timestamp(new Date(1_700_000_030_000L)),
                new Timestamp(new Date(1_700_000_040_000L)),
                Optional.empty());
        RecordingTransport transport = new RecordingTransport(
                Collections.singletonMap(vnodeTaskId(0), vnodeZeroState),
                Collections.emptyMap());

        addTasksAndStop(workerWithTransport(transport), tasks);

        assertEquals(2, transport.persistedStates.size());
        assertEquals(vnodeZeroState, transport.persistedStates.get(vnodeTaskId(0)));
        assertTrue(transport.persistedStates.containsKey(vnodeTaskId(1)));
        assertEquals(0, transport.migrationLookups);
    }

    @Test
    public void restoresLegacyCheckpointWhenConnectorTaskPartitionDoesNotContainTaskZero()
            throws Exception {
        Map<TaskId, SortedSet<StreamId>> tasks = new HashMap<>();
        tasks.put(tabletTaskId(1), singletonStream(tabletStream(2)));
        tasks.put(tabletTaskId(2), singletonStream(tabletStream(3)));
        TaskState legacyState = TaskState.createInitialFor(GENERATION, 10_000);
        RecordingTransport transport = new RecordingTransport(
                Collections.emptyMap(), Collections.singletonMap(legacyTaskId(), legacyState));

        addTasksAndStop(workerWithTransport(transport), tasks);

        assertEquals(Set.of(legacyTaskId()), transport.migrationLookup);
        assertEquals(2, transport.persistedStates.size());
        tasks.keySet().forEach(task ->
                assertEquals(legacyState, transport.persistedStates.get(task)));
    }

    @Test
    public void loadsUnassignedLegacyCheckpointThroughMigrationLookup() throws Exception {
        Map<TaskId, SortedSet<StreamId>> tasks = new HashMap<>();
        tasks.put(tabletTaskId(1), singletonStream(tabletStream(2)));
        TaskState legacyState = TaskState.createInitialFor(GENERATION, 10_000);
        RecordingTransport transport = new RecordingTransport(
                Collections.emptyMap(), Collections.singletonMap(legacyTaskId(), legacyState));

        addTasksAndStop(workerWithTransport(transport), tasks);

        assertEquals(tasks.keySet(), transport.assignedLookup);
        assertEquals(Set.of(legacyTaskId()), transport.migrationLookup);
        assertEquals(Collections.singletonMap(tabletTaskId(1), legacyState),
                transport.persistedStates);
    }

    @Test
    public void defaultMigrationLookupFailsFast() {
        Map<TaskId, SortedSet<StreamId>> tasks = Collections.singletonMap(
                tabletTaskId(1), singletonStream(tabletStream(2)));
        Worker worker = workerWithTransport(
                new AssignedStateOnlyTransport(Collections.emptyMap()));

        try {
            UnsupportedOperationException error = assertThrows(UnsupportedOperationException.class,
                    () -> worker.addTasks(new GroupedTasks(tasks, GENERATION)));
            assertTrue(error.getMessage().contains("must override getTaskStatesForMigration"));
        } finally {
            worker.stop();
        }
    }

    @Test
    public void checksForLegacyCheckpointWhenEveryPerStreamStateExists() throws Exception {
        Map<TaskId, SortedSet<StreamId>> tasks = new HashMap<>();
        tasks.put(tabletTaskId(0), singletonStream(tabletStream(1)));
        tasks.put(tabletTaskId(1), singletonStream(tabletStream(2)));
        TaskState firstState = TaskState.createInitialFor(GENERATION, 10_000);
        TaskState secondState = new TaskState(
                new Timestamp(new Date(1_700_000_030_000L)),
                new Timestamp(new Date(1_700_000_040_000L)),
                Optional.empty());
        Map<TaskId, TaskState> assignedStates = new HashMap<>();
        assignedStates.put(tabletTaskId(0), firstState);
        assignedStates.put(tabletTaskId(1), secondState);
        RecordingTransport transport = new RecordingTransport(
                assignedStates, Collections.emptyMap());

        addTasksAndStop(workerWithTransport(transport), tasks);

        assertEquals(assignedStates, transport.persistedStates);
        assertEquals(1, transport.migrationLookups);
        assertEquals(Set.of(legacyTaskId()), transport.migrationLookup);
    }

    @Test
    public void skipsMigrationLookupForVnodeTasks() throws Exception {
        Map<TaskId, SortedSet<StreamId>> tasks = Collections.singletonMap(
                vnodeTaskId(1), singletonStream(vnodeStream(2, 1)));
        RecordingTransport transport = new RecordingTransport(
                Collections.emptyMap(), Collections.emptyMap());

        addTasksAndStop(workerWithTransport(transport), tasks);

        assertEquals(0, transport.migrationLookups);
        assertEquals(tasks.keySet(), transport.persistedStates.keySet());
    }

    @Test
    public void supportedEmptyMigrationStartsWithoutRestoredState() throws Exception {
        Map<TaskId, SortedSet<StreamId>> tasks = Collections.singletonMap(
                tabletTaskId(1), singletonStream(tabletStream(2)));
        RecordingTransport transport = new RecordingTransport(
                Collections.emptyMap(), Collections.emptyMap());

        addTasksAndStop(workerWithTransport(transport), tasks);

        assertEquals(tasks.keySet(), transport.persistedStates.keySet());
    }

    @Test
    public void restoresMissingPartialPerStreamCheckpointsFromLegacyState() throws Exception {
        Map<TaskId, SortedSet<StreamId>> tasks = new HashMap<>();
        tasks.put(tabletTaskId(0), singletonStream(tabletStream(1)));
        tasks.put(tabletTaskId(1), singletonStream(tabletStream(2)));
        TaskState legacyState = TaskState.createInitialFor(GENERATION, 10_000);
        TaskState perStreamState = new TaskState(
                new Timestamp(new Date(1_700_000_030_000L)),
                new Timestamp(new Date(1_700_000_040_000L)),
                Optional.empty());
        RecordingTransport transport = new RecordingTransport(
                Collections.singletonMap(tabletTaskId(0), perStreamState),
                Collections.singletonMap(legacyTaskId(), legacyState));

        addTasksAndStop(workerWithTransport(transport), tasks);

        assertEquals(2, transport.persistedStates.size());
        assertEquals(perStreamState, transport.persistedStates.get(tabletTaskId(0)));
        assertEquals(legacyState, transport.persistedStates.get(tabletTaskId(1)));
    }

    @Test
    public void loadsLegacyCheckpointWhileAlsoProbingCompleteTablesForCleanup() throws Exception {
        TableName completeTable = new TableName("ks", "complete_table");
        TaskId completeTask = TaskId.forTabletStream(GENERATION, 0, completeTable);
        TaskId missingTask = tabletTaskId(1);
        Map<TaskId, SortedSet<StreamId>> tasks = new HashMap<>();
        tasks.put(completeTask, singletonStream(tabletStream(1)));
        tasks.put(missingTask, singletonStream(tabletStream(2)));
        TaskState completeState = TaskState.createInitialFor(GENERATION, 10_000);
        TaskState legacyState = new TaskState(
                new Timestamp(new Date(1_700_000_030_000L)),
                new Timestamp(new Date(1_700_000_040_000L)),
                Optional.empty());
        RecordingTransport transport = new RecordingTransport(
                Collections.singletonMap(completeTask, completeState),
                Collections.singletonMap(legacyTaskId(), legacyState));

        addTasksAndStop(workerWithTransport(transport), tasks);

        TaskId completeLegacyTask = new TaskId(GENERATION, new VNodeId(0), completeTable);
        assertEquals(Set.of(legacyTaskId(), completeLegacyTask), transport.migrationLookup);
        assertEquals(completeState, transport.persistedStates.get(completeTask));
        assertEquals(legacyState, transport.persistedStates.get(missingTask));
    }

    @Test
    public void rejectsMixedTabletAndVnodeLayoutForOneTable() {
        Map<TaskId, SortedSet<StreamId>> tasks = new HashMap<>();
        tasks.put(tabletTaskId(0), singletonStream(tabletStream(1)));
        tasks.put(vnodeTaskId(1), singletonStream(vnodeStream(2, 1)));

        RecordingTransport transport = new RecordingTransport(
                Collections.emptyMap(), Collections.emptyMap());
        Worker worker = workerWithTransport(transport);

        try {
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> worker.addTasks(new GroupedTasks(tasks, GENERATION)));
            assertTrue(error.getMessage().contains("No tasks in this assignment will start"));
        } finally {
            worker.stop();
        }

        assertTrue(transport.persistedStates.isEmpty());
    }

    @Test
    public void rejectsMixedTabletAndVnodeLayoutAcrossTaskGroups() {
        Map<TaskId, SortedSet<StreamId>> tabletTasks = Collections.singletonMap(
                tabletTaskId(0), singletonStream(tabletStream(1)));
        Map<TaskId, SortedSet<StreamId>> vnodeTasks = Collections.singletonMap(
                vnodeTaskId(1), singletonStream(vnodeStream(2, 1)));
        RecordingTransport transport = new RecordingTransport(
                Collections.emptyMap(), Collections.emptyMap());
        Worker worker = workerWithTransport(transport);

        try {
            assertThrows(IllegalArgumentException.class, () -> worker.runTaskGroups(List.of(
                    new GroupedTasks(tabletTasks, GENERATION),
                    new GroupedTasks(vnodeTasks, GENERATION))));
        } finally {
            worker.stop();
        }

        assertTrue(transport.setStates.isEmpty());
    }

    @Test
    public void completesMigrationOnlyAfterEveryReplacementStateIsStored() throws Exception {
        Map<TaskId, SortedSet<StreamId>> tasks = new HashMap<>();
        tasks.put(tabletTaskId(0), singletonStream(tabletStream(1)));
        tasks.put(tabletTaskId(1), singletonStream(tabletStream(2)));
        TaskState legacyState = TaskState.createInitialFor(GENERATION, 10_000);
        RecordingTransport transport = new RecordingTransport(
                Collections.emptyMap(), Collections.singletonMap(legacyTaskId(), legacyState));
        Worker worker = workerWithTransport(transport);

        try {
            worker.addTasks(new GroupedTasks(tasks, GENERATION));
        } finally {
            worker.stop();
        }

        assertEquals(Set.of(legacyTaskId()), transport.completedLegacyTasks);
        assertEquals(tasks.keySet(), transport.statesPresentAtCompletion);
    }

    @Test
    public void doesNotCompleteMigrationWhenAReplacementStateCannotBeStored() {
        Map<TaskId, SortedSet<StreamId>> tasks = new HashMap<>();
        tasks.put(tabletTaskId(0), singletonStream(tabletStream(1)));
        tasks.put(tabletTaskId(1), singletonStream(tabletStream(2)));
        TaskState legacyState = TaskState.createInitialFor(GENERATION, 10_000);
        RecordingTransport transport = new RecordingTransport(
                Collections.emptyMap(), Collections.singletonMap(legacyTaskId(), legacyState));
        transport.failOnSetStateInvocation = 2;
        Worker worker = workerWithTransport(transport);

        try {
            assertThrows(IllegalStateException.class,
                    () -> worker.addTasks(new GroupedTasks(tasks, GENERATION)));
        } finally {
            worker.stop();
        }

        assertTrue(transport.completedLegacyTasks.isEmpty());
    }

    private static void addTasksAndStop(
            Worker worker, Map<TaskId, SortedSet<StreamId>> tasks) throws Exception {
        try {
            worker.addTasks(new GroupedTasks(tasks, GENERATION));
        } finally {
            worker.stop();
        }
    }

    private static Worker workerWithTransport(WorkerTransport transport) {
        long generationStartMs = GENERATION.getGenerationStart().toDate().getTime();
        return workerWithTransport(transport, new MockWorkerCQL(),
                Consumer.syncRawChangeConsumer(change -> { }),
                Clock.fixed(Instant.ofEpochMilli(generationStartMs + 30_000), ZoneOffset.UTC));
    }

    private static Worker workerWithTransport(WorkerTransport transport, MockWorkerCQL cql,
                                              Consumer consumer, Clock clock) {
        return new Worker(WorkerConfiguration.builder()
                .withCQL(cql)
                .withTransport(transport)
                .withConsumer(consumer)
                .withQueryTimeWindowSizeMs(10_000)
                .withConfidenceWindowSizeMs(1)
                .withClock(clock)
                .build());
    }

    private abstract static class NoOpTransport implements WorkerTransport {
        @Override
        public void setState(TaskId task, TaskState newState) {
        }

        @Override
        public void updateState(TaskId task, TaskState newState) {
        }

        @Override
        public void moveStateToNextWindow(TaskId task, TaskState newState) {
        }
    }

    private static final class AssignedStateOnlyTransport extends NoOpTransport {
        private final Map<TaskId, TaskState> assignedStates;

        private AssignedStateOnlyTransport(Map<TaskId, TaskState> assignedStates) {
            this.assignedStates = assignedStates;
        }

        @Override
        public Map<TaskId, TaskState> getTaskStates(Set<TaskId> taskIds) {
            return assignedStates;
        }
    }

    private static final class RecordingTransport extends NoOpTransport {
        private final Map<TaskId, TaskState> assignedStates;
        private final Map<TaskId, TaskState> migrationStates;
        private final List<TaskId> setStates = new ArrayList<>();
        private final Map<TaskId, TaskState> persistedStates = new HashMap<>();
        private Set<TaskId> assignedLookup = Collections.emptySet();
        private Set<TaskId> migrationLookup = Collections.emptySet();
        private Set<TaskId> completedLegacyTasks = Collections.emptySet();
        private Set<TaskId> statesPresentAtCompletion = Collections.emptySet();
        private int migrationLookups;
        private int failOnSetStateInvocation = -1;

        private RecordingTransport(Map<TaskId, TaskState> assignedStates,
                                   Map<TaskId, TaskState> migrationStates) {
            this.assignedStates = assignedStates;
            this.migrationStates = migrationStates;
        }

        @Override
        public Map<TaskId, TaskState> getTaskStates(Set<TaskId> taskIds) {
            assignedLookup = Set.copyOf(taskIds);
            return assignedStates;
        }

        @Override
        public Map<TaskId, TaskState> getTaskStatesForMigration(Set<TaskId> taskIds) {
            migrationLookups++;
            migrationLookup = Set.copyOf(taskIds);
            Map<TaskId, TaskState> result = new HashMap<>();
            taskIds.forEach(taskId -> {
                TaskState state = migrationStates.get(taskId);
                if (state != null) {
                    result.put(taskId, state);
                }
            });
            return result;
        }

        @Override
        public void setState(TaskId task, TaskState newState) {
            if (setStates.size() + 1 == failOnSetStateInvocation) {
                throw new IllegalStateException("injected setState failure");
            }
            setStates.add(task);
            persistedStates.put(task, newState);
        }

        @Override
        public void completeTaskStateMigration(Set<TaskId> legacyTasks) {
            completedLegacyTasks = Set.copyOf(legacyTasks);
            statesPresentAtCompletion = Set.copyOf(setStates);
        }
    }

    private static TaskId tabletTaskId(int index) {
        return TaskId.forTabletStream(GENERATION, index, TABLE);
    }

    private static TaskId vnodeTaskId(int index) {
        return new TaskId(GENERATION, new VNodeId(index), TABLE);
    }

    private static TaskId legacyTaskId() {
        return vnodeTaskId(0);
    }

    private static SortedSet<StreamId> singletonStream(StreamId stream) {
        return new TreeSet<>(Collections.singleton(stream));
    }

    private static MockRawChange rawChange(StreamId stream, long timestampMs) {
        return MockRawChange.builder()
                .withChangeSchema(TEST_SCHEMA_SIMPLE)
                .withStreamId(stream.getValue())
                .withTimeMs(timestampMs)
                .build();
    }

    private static List<RawChange> changesForStream(
            List<RawChange> changes, StreamId stream) {
        return changes.stream()
                .filter(change -> change.getId().getStreamId().equals(stream))
                .collect(Collectors.toList());
    }

    private static StreamId tabletStream(long token) {
        return vnodeStream(token, 0);
    }

    private static StreamId vnodeStream(long token, int vnodeIndex) {
        ByteBuffer value = ByteBuffer.allocate(16);
        value.putLong(token);
        value.putLong(((long) vnodeIndex << 4) | 1L);
        value.flip();
        return new StreamId(value);
    }
}
