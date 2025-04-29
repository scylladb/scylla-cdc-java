package com.scylladb.cdc.model.worker;

import com.google.common.collect.Lists;
import com.scylladb.cdc.cql.MockWorkerCQL;
import com.scylladb.cdc.cql.error.worker.ContinuousChangeErrorInject;
import com.scylladb.cdc.cql.error.worker.OnceChangeErrorInject;
import com.scylladb.cdc.model.*;
import com.scylladb.cdc.model.master.GenerationMetadata;
import com.scylladb.cdc.model.master.MockGenerationMetadata;
import com.scylladb.cdc.transport.MockWorkerTransport;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.scylladb.cdc.model.worker.WorkerThread.DEFAULT_CONFIDENCE_WINDOW_SIZE_MS;
import static com.scylladb.cdc.model.worker.WorkerThread.DEFAULT_QUERY_WINDOW_SIZE_MS;
import static org.awaitility.Awaitility.with;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class WorkerTest {
    private static final long DEFAULT_AWAIT_TIMEOUT_MS = 2000;
    private static final ConditionFactory DEFAULT_AWAIT =
            with().pollInterval(1, TimeUnit.MILLISECONDS).await()
                    .atMost(DEFAULT_AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    private static long TEST_GENERATION_START_MS = 5 * 60 * 1000;
    private static int TEST_GENERATION_VNODE_COUNT = 16;
    private static int TEST_GENERATION_STREAMS_PER_VNODE_COUNT = 4;
    private static GenerationMetadata TEST_GENERATION = MockGenerationMetadata.mockGenerationMetadata(
            new Timestamp(new Date(TEST_GENERATION_START_MS)), Optional.empty(),
            TEST_GENERATION_VNODE_COUNT, TEST_GENERATION_STREAMS_PER_VNODE_COUNT);

    protected static TableName TEST_TABLE_NAME = new TableName("ks", "t");

    // ChangeSchema for table:
    // CREATE TABLE ks.t(pk int, ck int, v int, PRIMARY KEY(pk, ck)) WITH cdc = {'enabled': true};
    protected static ChangeSchema TEST_CHANGE_SCHEMA = new ChangeSchema(Lists.newArrayList(
            new ChangeSchema.ColumnDefinition("cdc$stream_id", 0, new ChangeSchema.DataType(ChangeSchema.CqlType.BLOB), null, null),
            new ChangeSchema.ColumnDefinition("cdc$time", 1, new ChangeSchema.DataType(ChangeSchema.CqlType.TIMEUUID), null, null),
            new ChangeSchema.ColumnDefinition("cdc$batch_seq_no", 2, new ChangeSchema.DataType(ChangeSchema.CqlType.INT), null, null),
            new ChangeSchema.ColumnDefinition("cdc$deleted_v", 3, new ChangeSchema.DataType(ChangeSchema.CqlType.BOOLEAN), null, null),
            new ChangeSchema.ColumnDefinition("cdc$end_of_batch", 4, new ChangeSchema.DataType(ChangeSchema.CqlType.BOOLEAN), null, null),
            new ChangeSchema.ColumnDefinition("cdc$operation", 5, new ChangeSchema.DataType(ChangeSchema.CqlType.TINYINT), null, null),
            new ChangeSchema.ColumnDefinition("cdc$ttl", 6, new ChangeSchema.DataType(ChangeSchema.CqlType.BIGINT), null, null),
            new ChangeSchema.ColumnDefinition("ck", 7, new ChangeSchema.DataType(ChangeSchema.CqlType.INT), new ChangeSchema.DataType(ChangeSchema.CqlType.INT), ChangeSchema.ColumnKind.CLUSTERING_KEY),
            new ChangeSchema.ColumnDefinition("pk", 8, new ChangeSchema.DataType(ChangeSchema.CqlType.INT), new ChangeSchema.DataType(ChangeSchema.CqlType.INT), ChangeSchema.ColumnKind.PARTITION_KEY),
            new ChangeSchema.ColumnDefinition("v", 9, new ChangeSchema.DataType(ChangeSchema.CqlType.INT), new ChangeSchema.DataType(ChangeSchema.CqlType.INT), ChangeSchema.ColumnKind.REGULAR)
    ));

    @Test
    public void testWorkerReadsAnyWindows() {
        // Worker should start reading windows
        // from the beginning of the generation.

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, noOpConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                    TEST_GENERATION_START_MS, TEST_GENERATION_START_MS + DEFAULT_QUERY_WINDOW_SIZE_MS)));

            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                    TEST_GENERATION_START_MS + DEFAULT_QUERY_WINDOW_SIZE_MS,
                    TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS)));

            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                    TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS,
                    TEST_GENERATION_START_MS + 3 * DEFAULT_QUERY_WINDOW_SIZE_MS)));

            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 3, TEST_TABLE_NAME,
                    TEST_GENERATION_START_MS, TEST_GENERATION_START_MS + DEFAULT_QUERY_WINDOW_SIZE_MS)));
        }
    }

    @Test
    public void testWorkerWaitForWindow() {
        // Worker should start reading windows
        // from the beginning of the generation
        // AND should respect the minimal wait time for
        // reading next window.

        final long TEST_MINIMAL_WAIT_FOR_WINDOW_MS = 5*1000;
        // To prevent exceptions like org.awaitility.core.ConditionTimeoutException:
        // Condition was evaluated in 9 seconds 999 milliseconds which is earlier than expected minimum timeout 10 seconds
        final long EPSILON_MS = 15;


        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});
        WorkerConfiguration.Builder builder = WorkerConfiguration.builder();
        builder.withCQL(mockWorkerCQL)
            .withTransport(workerTransport)
            .withConsumer(noOpConsumer)
            .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
            .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
            .withClock(Clock.systemDefaultZone())
            .withMinimalWaitForWindowMs(TEST_MINIMAL_WAIT_FOR_WINDOW_MS)
            .build();
        Map<TaskId, SortedSet<StreamId>> groupedStreams =
            MockGenerationMetadata.generationMetadataToTaskMap(TEST_GENERATION, Collections.singleton(TEST_TABLE_NAME));

        ConditionFactory customAwait =
            with().pollInterval(1, TimeUnit.MILLISECONDS).await()
                .atLeast(TEST_MINIMAL_WAIT_FOR_WINDOW_MS - EPSILON_MS, TimeUnit.MILLISECONDS)
                .atMost(TEST_MINIMAL_WAIT_FOR_WINDOW_MS + DEFAULT_AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        try (WorkerThread workerThread = new WorkerThread(builder.build(), groupedStreams)) {
            customAwait.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                TEST_GENERATION_START_MS, TEST_GENERATION_START_MS + DEFAULT_QUERY_WINDOW_SIZE_MS)));
            customAwait.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                TEST_GENERATION_START_MS + DEFAULT_QUERY_WINDOW_SIZE_MS,
                TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS)));
            customAwait.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS,
                TEST_GENERATION_START_MS + 3 * DEFAULT_QUERY_WINDOW_SIZE_MS)));
            customAwait.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                TEST_GENERATION_START_MS + 3 * DEFAULT_QUERY_WINDOW_SIZE_MS,
                TEST_GENERATION_START_MS + 4 * DEFAULT_QUERY_WINDOW_SIZE_MS)));
        }
    }

    @Test
    public void testWorkerTrimsWindowWithTTL() {
        // If a TTL was set on a table (as is the case
        // in Scylla by default on the CDC log table), Worker
        // should not start reading from the beginning of
        // generation, but from |now - ttl|.

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();

        // "Now" will be at second 935 and 146 second TTL:
        long now = 935, ttl = 146;
        mockWorkerCQL.setTablesTTL(Collections.singletonMap(TEST_TABLE_NAME, Optional.of(ttl)));
        Clock clock = Clock.fixed(Instant.ofEpochSecond(now), ZoneOffset.systemDefault());

        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, noOpConsumer, TEST_GENERATION, clock, TEST_TABLE_NAME)) {

            // Expecting to see a window queried from time point (now - ttl).
            // Multiplying by 1000 to convert from seconds to milliseconds.
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                    (now - ttl) * 1000, (now - ttl) * 1000 + DEFAULT_QUERY_WINDOW_SIZE_MS)));

            // And no window before that:
            assertFalse(mockWorkerCQL.wasCreateReaderInvoked(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                    (now - ttl) * 1000 - DEFAULT_QUERY_WINDOW_SIZE_MS, (now - ttl) * 1000)));
        }
    }

    @Test
    public void testWorkerTrimsSavedTaskStatesWithTTL() {
        // If a TTL is set on a table, but
        // there are TaskStates saved on the transport,
        // they still should be trimmed with the TTL value.

        // "Now" will be at second 935 and 146 second TTL:
        long now = 935, ttl = 146;
        Clock clock = Clock.fixed(Instant.ofEpochSecond(now), ZoneOffset.systemDefault());

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        // Previously saved TaskStates:
        // task1 - before TTL
        // task2 - after TTL
        // task3 - intersecting with TTL
        Task task1 = generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                TEST_GENERATION_START_MS + 18 * DEFAULT_QUERY_WINDOW_SIZE_MS,
                TEST_GENERATION_START_MS + 19 * DEFAULT_QUERY_WINDOW_SIZE_MS);
        Task task2 = generateTask(TEST_GENERATION, 1, TEST_TABLE_NAME,
                900 * 1000, 900 * 1000 + DEFAULT_QUERY_WINDOW_SIZE_MS);
        Task task3 = generateTask(TEST_GENERATION, 2, TEST_TABLE_NAME,
                (now - ttl) * 1000 - 3,
                (now - ttl) * 1000 - 3 + DEFAULT_QUERY_WINDOW_SIZE_MS);
        // FIXME: add a test with TaskState with lastConsumedId

        workerTransport.setState(task1.id, task1.state);
        workerTransport.setState(task2.id, task2.state);
        workerTransport.setState(task3.id, task3.state);

        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        mockWorkerCQL.setTablesTTL(Collections.singletonMap(TEST_TABLE_NAME, Optional.of(ttl)));

        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, noOpConsumer, TEST_GENERATION, clock, TEST_TABLE_NAME)) {
            // task1 - before TTL, so Worker should start reading
            // from TTL, "discarding" task1.
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                    (now - ttl) * 1000, (now - ttl) * 1000 + DEFAULT_QUERY_WINDOW_SIZE_MS)));
            assertFalse(mockWorkerCQL.wasCreateReaderInvoked(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                    (now - ttl) * 1000 - DEFAULT_QUERY_WINDOW_SIZE_MS, (now - ttl) * 1000)));

            // task2 - after TTL, so Worker should start reading
            // from it.
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(task2));
            assertFalse(mockWorkerCQL.wasCreateReaderInvoked(task2.updateState(task2.state.moveToNextWindow(-DEFAULT_QUERY_WINDOW_SIZE_MS))));

            // task3 - intersecting with TTL. For simplicity,
            // the Worker does not trim it and starts from it.
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(task3));
            assertFalse(mockWorkerCQL.wasCreateReaderInvoked(task3.updateState(task3.state.moveToNextWindow(-DEFAULT_QUERY_WINDOW_SIZE_MS))));
        }
    }

    @Test
    public void testWorkerConsumesSingleVNodeChangesInOrder() {
        // Worker should consume changes within a single
        // vnode in order of stream id and timestamp.

        MockRawChange change1 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 0)
                .withTimeMs(TEST_GENERATION_START_MS + 12)
                .addPrimaryKey("pk", 1)
                .addPrimaryKey("ck", 2)
                .addAtomicRegularColumn("v", 3)
                .build();

        MockRawChange change2 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 1)
                .withTimeMs(TEST_GENERATION_START_MS + 12)
                .addPrimaryKey("pk", 4)
                .addPrimaryKey("ck", 5)
                .addAtomicRegularColumn("v", 6)
                .build();

        MockRawChange change3 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 2)
                .withTimeMs(TEST_GENERATION_START_MS + 13)
                .addPrimaryKey("pk", 7)
                .addPrimaryKey("ck", 8)
                .addAtomicRegularColumn("v", 9)
                .build();

        MockRawChange change4 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 0)
                .withTimeMs(TEST_GENERATION_START_MS + 19)
                .addPrimaryKey("pk", 10)
                .addPrimaryKey("ck", 11)
                .addAtomicRegularColumn("v", 12)
                .build();

        MockRawChange change5 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 3)
                .withTimeMs(TEST_GENERATION_START_MS + 100)
                .addPrimaryKey("pk", 13)
                .addPrimaryKey("ck", 14)
                .addAtomicRegularColumn("v", 15)
                .build();

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        List<MockRawChange> rawChanges = Lists.newArrayList(change1, change2, change3, change4, change5);
        mockWorkerCQL.setRawChanges(rawChanges);
        List<RawChange> observedChanges = Collections.synchronizedList(new ArrayList<>());
        Consumer accumulatingConsumer = Consumer.syncRawChangeConsumer(observedChanges::add);

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, accumulatingConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            DEFAULT_AWAIT.until(() -> observedChanges.equals(rawChanges));
        }
    }

    @Test
    public void testWorkerConsumesMultiVNodeChanges() {
        // Worker should be able to read changes
        // from all vnodes, many windows.

        List<MockRawChange> rawChanges = new ArrayList<>();
        for (int vnode = 0; vnode < 16; vnode++) {
            rawChanges.add(MockRawChange.builder()
                    .withChangeSchema(TEST_CHANGE_SCHEMA)
                    .withStreamId(TEST_GENERATION, vnode, 2)
                    .withTimeMs(TEST_GENERATION_START_MS + 17)
                    .addPrimaryKey("pk", 15)
                    .addPrimaryKey("ck", 1)
                    .addAtomicRegularColumn("v", 7)
                    .build());

            rawChanges.add(MockRawChange.builder()
                    .withChangeSchema(TEST_CHANGE_SCHEMA)
                    .withStreamId(TEST_GENERATION, vnode, 1)
                    .withTimeMs(TEST_GENERATION_START_MS + 17)
                    .addPrimaryKey("pk", 11)
                    .addPrimaryKey("ck", vnode)
                    .addAtomicRegularColumn("v", 3)
                    .build());

            rawChanges.add(MockRawChange.builder()
                    .withChangeSchema(TEST_CHANGE_SCHEMA)
                    .withStreamId(TEST_GENERATION, vnode, 1)
                    .withTimeMs(TEST_GENERATION_START_MS + 44)
                    .addPrimaryKey("pk", vnode)
                    .addPrimaryKey("ck", 2)
                    .addAtomicRegularColumn("v", 3)
                    .build());

            rawChanges.add(MockRawChange.builder()
                    .withChangeSchema(TEST_CHANGE_SCHEMA)
                    .withStreamId(TEST_GENERATION, vnode, 3)
                    .withTimeMs(TEST_GENERATION_START_MS + 48 + vnode * 3)
                    .addPrimaryKey("pk", 12)
                    .addPrimaryKey("ck", 5)
                    .addAtomicRegularColumn("v", vnode)
                    .build());
        }

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        mockWorkerCQL.setRawChanges(rawChanges);
        List<RawChange> observedChanges = Collections.synchronizedList(new ArrayList<>());
        Consumer accumulatingConsumer = Consumer.syncRawChangeConsumer(observedChanges::add);

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, accumulatingConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            DEFAULT_AWAIT.until(() -> observedChanges.containsAll(rawChanges));
            // No duplicates:
            assertEquals(observedChanges.size(), rawChanges.size());
        }
    }

    @Test
    public void testWorkerConsumesChangesFromSavedTaskState() {
        // Worker should start reading changes
        // from the TaskState in Transport,
        // and skip changes before that state.

        MockRawChange change1 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 0)
                .withTimeMs(TEST_GENERATION_START_MS + 12)
                .addPrimaryKey("pk", 1)
                .addPrimaryKey("ck", 2)
                .addAtomicRegularColumn("v", 3)
                .build();

        MockRawChange change2 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 1)
                .withTimeMs(TEST_GENERATION_START_MS + 12)
                .addPrimaryKey("pk", 4)
                .addPrimaryKey("ck", 5)
                .addAtomicRegularColumn("v", 6)
                .build();

        MockRawChange change3 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 2)
                .withTimeMs(TEST_GENERATION_START_MS + 13)
                .addPrimaryKey("pk", 7)
                .addPrimaryKey("ck", 8)
                .addAtomicRegularColumn("v", 9)
                .build();

        MockRawChange change4 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 0)
                .withTimeMs(TEST_GENERATION_START_MS + 19)
                .addPrimaryKey("pk", 10)
                .addPrimaryKey("ck", 11)
                .addAtomicRegularColumn("v", 12)
                .build();

        MockRawChange change5 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 3)
                .withTimeMs(TEST_GENERATION_START_MS + 100)
                .addPrimaryKey("pk", 13)
                .addPrimaryKey("ck", 14)
                .addAtomicRegularColumn("v", 15)
                .build();

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        Task savedTask = generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                TEST_GENERATION_START_MS + 3 * DEFAULT_QUERY_WINDOW_SIZE_MS,
                TEST_GENERATION_START_MS + 4 * DEFAULT_QUERY_WINDOW_SIZE_MS);
        workerTransport.setState(savedTask.id, savedTask.state);

        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        List<MockRawChange> rawChanges = Lists.newArrayList(change1, change2, change3, change4, change5);
        mockWorkerCQL.setRawChanges(rawChanges);
        List<RawChange> observedChanges = Collections.synchronizedList(new ArrayList<>());
        Consumer accumulatingConsumer = Consumer.syncRawChangeConsumer(observedChanges::add);

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, accumulatingConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            DEFAULT_AWAIT.until(() -> observedChanges.equals(rawChanges.subList(3, 4)));
        }

        // Test with another savedTask:
        savedTask = generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS,
                TEST_GENERATION_START_MS + 3 * DEFAULT_QUERY_WINDOW_SIZE_MS);
        savedTask = savedTask.updateState(savedTask.state.update(change1.getId()));
        workerTransport.setState(savedTask.id, savedTask.state);
        observedChanges.clear();

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, accumulatingConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            DEFAULT_AWAIT.until(() -> observedChanges.equals(rawChanges.subList(1, 4)));
        }
    }

    @Test
    public void testWorkerSavesMovedWindowStateToTransport() {
        // Worker should call save state after each
        // moving of window.

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        // The first window is not saved using moveStateToNextWindow.
        Task secondWindow = generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                TEST_GENERATION_START_MS + DEFAULT_QUERY_WINDOW_SIZE_MS, TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS);
        Task thirdWindow = generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS, TEST_GENERATION_START_MS + 3 * DEFAULT_QUERY_WINDOW_SIZE_MS);

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, noOpConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            // Wait for the third window to be committed to transport.
            DEFAULT_AWAIT.until(() -> workerTransport.getMoveStateToNextWindowInvocations(thirdWindow.id).size() >= 2);
        }

        // Worker has now stopped, check if the transport
        // received moveStateToNextWindow for the second
        // and third window.

        List<TaskState> windows = workerTransport.getMoveStateToNextWindowInvocations(secondWindow.id).subList(0, 2);
        assertEquals(Lists.newArrayList(secondWindow.state, thirdWindow.state), windows);
    }

    @Test
    public void testWorkerSavesWithinWindowStateToTransport() {
        // Worker should call save state after
        // each successful reading of a change.

        MockRawChange change1 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 0)
                .withTimeMs(TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 1)
                .addPrimaryKey("pk", 1)
                .addPrimaryKey("ck", 2)
                .addAtomicRegularColumn("v", 3)
                .build();

        MockRawChange change2 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 0)
                .withTimeMs(TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 2)
                .addPrimaryKey("pk", 4)
                .addPrimaryKey("ck", 5)
                .addAtomicRegularColumn("v", 6)
                .build();

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        List<MockRawChange> rawChanges = Lists.newArrayList(change1, change2);
        mockWorkerCQL.setRawChanges(rawChanges);
        List<RawChange> observedChanges = Collections.synchronizedList(new ArrayList<>());
        Consumer accumulatingConsumer = Consumer.syncRawChangeConsumer(observedChanges::add);

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, accumulatingConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            DEFAULT_AWAIT.until(() -> observedChanges.equals(rawChanges));
        }

        // The consumer has received all changes,
        // let's check whether it updated the windows correctly.
        Task windowReadTask = generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS,
                TEST_GENERATION_START_MS + 3 * DEFAULT_QUERY_WINDOW_SIZE_MS);

        // Skip a few windows unrelated to windowReadTask:
        List<TaskState> windows = workerTransport.getSetStateInvocations(windowReadTask.id).subList(2, 5);

        TaskState windowBeginningState = windowReadTask.state;
        TaskState afterChange1State = windowReadTask.state.update(change1.getId());
        TaskState afterChange2State = windowReadTask.state.update(change2.getId());

        assertEquals(Lists.newArrayList(windowBeginningState, afterChange1State, afterChange2State), windows);
    }

    @Test
    public void testWorkerRetriesFailedConsumer() {
        // Test that Worker retries after
        // exception from Consumer and
        // doesn't re-read changes.

        MockRawChange change1 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 0)
                .withTimeMs(TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 1)
                .addPrimaryKey("pk", 1)
                .addPrimaryKey("ck", 2)
                .addAtomicRegularColumn("v", 3)
                .build();

        MockRawChange change2 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 0)
                .withTimeMs(TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 2)
                .addPrimaryKey("pk", 4)
                .addPrimaryKey("ck", 5)
                .addAtomicRegularColumn("v", 6)
                .build();

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        List<MockRawChange> rawChanges = Lists.newArrayList(change1, change2);
        mockWorkerCQL.setRawChanges(rawChanges);

        // Set up a consumer that starts
        // failing after the first change
        // until failure mode is stopped.
        AtomicBoolean shouldFail = new AtomicBoolean(false);
        AtomicInteger failCount = new AtomicInteger(0);
        List<RawChange> observedChanges = Collections.synchronizedList(new ArrayList<>());
        Consumer failingConsumer = Consumer.forRawChangeConsumer(change -> {
            if (shouldFail.get()) {
                failCount.incrementAndGet();
                CompletableFuture<Void> injectedExceptionFuture = new CompletableFuture<>();
                injectedExceptionFuture.completeExceptionally(new RuntimeException("Injected exception in failingConsumer"));
                return injectedExceptionFuture;
            }
            if (change.equals(change1)) {
                // Start failing after the first change
                shouldFail.set(true);
            }
            observedChanges.add(change);
            return CompletableFuture.completedFuture(null);
        });

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, failingConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            DEFAULT_AWAIT.until(() -> failCount.get() > 3);

            // We should have only succeeded in reading the first change.
            assertEquals(Collections.singletonList(change1), observedChanges);

            shouldFail.set(false);

            DEFAULT_AWAIT.until(() -> rawChanges.equals(observedChanges));
        }
    }

    @Test
    public void testWorkerRetriesSingleCQLException() {
        // Test that Worker correctly handles
        // a single WorkerCQL nextChange() exception.

        MockRawChange change1 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 0)
                .withTimeMs(TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 1)
                .addPrimaryKey("pk", 1)
                .addPrimaryKey("ck", 2)
                .addAtomicRegularColumn("v", 3)
                .build();

        MockRawChange change2 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 0)
                .withTimeMs(TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 2)
                .addPrimaryKey("pk", 4)
                .addPrimaryKey("ck", 5)
                .addAtomicRegularColumn("v", 6)
                .build();

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        List<MockRawChange> rawChanges = Lists.newArrayList(change1, change2);
        mockWorkerCQL.setRawChanges(rawChanges);
        List<RawChange> observedChanges = Collections.synchronizedList(new ArrayList<>());
        Consumer accumulatingConsumer = Consumer.syncRawChangeConsumer(observedChanges::add);

        // Inject a single failure.
        OnceChangeErrorInject errorInjection = new OnceChangeErrorInject();
        errorInjection.requestRawChangeError(change1);
        mockWorkerCQL.setCQLErrorStrategy(errorInjection);

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, accumulatingConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.getFailureCount() == 1);
            DEFAULT_AWAIT.until(() -> observedChanges.equals(rawChanges));
        }
    }

    @Test
    public void testWorkerSurvivesFailureAndRestart() {
        // Test that Worker correctly handles the following
        // scenario: successful reading of a few changes,
        // then constant CQL failure, restart and successful
        // reading of next changes.

        MockRawChange change1 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 0)
                .withTimeMs(TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 1)
                .addPrimaryKey("pk", 1)
                .addPrimaryKey("ck", 2)
                .addAtomicRegularColumn("v", 3)
                .build();

        MockRawChange change2 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 0)
                .withTimeMs(TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 2)
                .addPrimaryKey("pk", 4)
                .addPrimaryKey("ck", 5)
                .addAtomicRegularColumn("v", 6)
                .build();

        MockRawChange change3 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 0)
                .withTimeMs(TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 3)
                .addPrimaryKey("pk", 1)
                .addPrimaryKey("ck", 9)
                .addAtomicRegularColumn("v", 4)
                .build();

        MockRawChange change4 = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(TEST_GENERATION, 0, 0)
                .withTimeMs(TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 4)
                .addPrimaryKey("pk", 2)
                .addPrimaryKey("ck", 9)
                .addAtomicRegularColumn("v", 11)
                .build();

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        List<MockRawChange> rawChanges = Lists.newArrayList(change1, change2, change3, change4);
        mockWorkerCQL.setRawChanges(rawChanges);
        List<RawChange> observedChanges = Collections.synchronizedList(new ArrayList<>());
        Consumer accumulatingConsumer = Consumer.syncRawChangeConsumer(observedChanges::add);

        // Inject a "constant" failure at change 3.
        ContinuousChangeErrorInject errorStrategy = new ContinuousChangeErrorInject();
        errorStrategy.requestRawChangeError(change3);
        mockWorkerCQL.setCQLErrorStrategy(errorStrategy);

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, accumulatingConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            // Wait for a CQL failure.
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.getFailureCount() > 2);

            // We will only successfully have read the first two changes.
            DEFAULT_AWAIT.until(() -> observedChanges.equals(Lists.newArrayList(change1, change2)));
        }

        // Check if the transport has the correct TaskState.
        Task failedTask = generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS,
                TEST_GENERATION_START_MS + 3 * DEFAULT_QUERY_WINDOW_SIZE_MS);
        TaskState fetchedTaskState = workerTransport.getTaskStates(Collections.singleton(failedTask.id)).get(failedTask.id);
        assertEquals(fetchedTaskState, failedTask.state.update(change2.getId()));

        // Restart Worker.
        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, accumulatingConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            // Let it fail a few times more.

            int initialFailureCount = mockWorkerCQL.getFailureCount();
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.getFailureCount() > initialFailureCount + 2);

            // Cancel the failure, making it possible to read all changes.
            errorStrategy.cancelRequestRawChangeError(change3);

            DEFAULT_AWAIT.until(() -> observedChanges.equals(rawChanges));
        }
    }

    private Task generateTask(GenerationMetadata generationMetadata, int vnodeIndex, TableName tableName,
                              long windowStartMs, long windowEndMs) {
        VNodeId vnodeId = new VNodeId(vnodeIndex);
        TaskId taskId = new TaskId(generationMetadata.getId(), vnodeId, tableName);

        SortedSet<StreamId> streamIds = generationMetadata.getStreams().stream()
                .filter(s -> s.getVNodeId().equals(vnodeId))
                .collect(Collectors.toCollection(TreeSet::new));

        TaskState taskState = new TaskState(new Timestamp(new Date(windowStartMs)),
                new Timestamp(new Date(windowEndMs)), Optional.empty());
        return new Task(taskId, streamIds, taskState);
    }
}
