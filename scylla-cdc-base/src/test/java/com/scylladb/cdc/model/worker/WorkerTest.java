package com.scylladb.cdc.model.worker;

import com.google.common.collect.Lists;
import com.scylladb.cdc.cql.MockWorkerCQL;
import com.scylladb.cdc.cql.error.worker.ContinuousChangeErrorInject;
import com.scylladb.cdc.cql.error.worker.OnceChangeErrorInject;
import com.scylladb.cdc.model.*;
import com.scylladb.cdc.model.master.GenerationMetadata;
import com.scylladb.cdc.model.master.MockGenerationMetadata;
import com.scylladb.cdc.transport.MockWorkerTransport;
import com.scylladb.cdc.transport.GroupedTasks;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        GroupedTasks groupedStreams =
            MockGenerationMetadata.generationMetadataToWorkerTasks(TEST_GENERATION, Collections.singleton(TEST_TABLE_NAME));

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
        List<TaskState> windows = workerTransport.getUpdateStateInvocations(windowReadTask.id).subList(2, 5);

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

    @Test
    public void testWorkerProbeSkipsEmptyWindowsOnCatchUp() {
        // When catchUpWindowSizeSeconds > 0, generation is closed, and initial
        // window is far in the past, worker should use probe to skip ahead.

        // Set a closed generation ending at a known time
        long genStartMs = 5 * 60 * 1000; // 5 minutes
        long genEndMs = 10 * 60 * 1000;  // 10 minutes
        GenerationMetadata closedGeneration = MockGenerationMetadata.mockGenerationMetadata(
                new Timestamp(new Date(genStartMs)), Optional.of(new Timestamp(new Date(genEndMs))),
                1, 1);

        // "Now" is at minute 32, catchUp = 3 minutes → cutoff at minute 29
        long nowMs = 32 * 60 * 1000;
        long catchUpSeconds = 3 * 60;
        Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.systemDefault());

        // Create a change at 7 minutes
        long changeTimeMs = 7 * 60 * 1000;
        StreamId firstStream = closedGeneration.getStreams().first();
        MockRawChange change = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(firstStream.getValue())
                .withTimeMs(changeTimeMs)
                .addPrimaryKey("pk", 1)
                .addPrimaryKey("ck", 2)
                .addAtomicRegularColumn("v", 3)
                .build();

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        List<MockRawChange> rawChanges = new ArrayList<>();
        rawChanges.add(change);
        mockWorkerCQL.setRawChanges(rawChanges);

        List<RawChange> observedChanges = Collections.synchronizedList(new ArrayList<>());
        Consumer accumulatingConsumer = Consumer.syncRawChangeConsumer(observedChanges::add);

        WorkerConfiguration config = WorkerConfiguration.builder()
                .withCQL(mockWorkerCQL)
                .withTransport(workerTransport)
                .withConsumer(accumulatingConsumer)
                .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
                .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
                .withClock(clock)
                .withCatchUpWindowSizeSeconds(catchUpSeconds)
                .build();

        GroupedTasks groupedTasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                closedGeneration, Collections.singleton(TEST_TABLE_NAME));

        try (WorkerThread workerThread = new WorkerThread(config, groupedTasks)) {
            DEFAULT_AWAIT.until(() -> observedChanges.contains(change));
        }
    }

    @Test
    public void testWorkerProbeDisabledByDefault() {
        // With catchUpWindowSizeSeconds = 0 (default), no probes should fire.
        // Worker should start reading from the generation start as usual.
        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        // Default config (catchUpWindowSizeSeconds = 0)
        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, noOpConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            // Worker should read from the very start of the generation
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                    TEST_GENERATION_START_MS, TEST_GENERATION_START_MS + DEFAULT_QUERY_WINDOW_SIZE_MS)));
        }
    }

    @Test
    public void testWorkerProbeSkippedForOpenGeneration() {
        // Open generation → no probes even with catch-up enabled.
        // TEST_GENERATION is already open (no end timestamp).
        long nowMs = 32 * 60 * 1000;
        long catchUpSeconds = 3 * 60;
        Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.systemDefault());

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        WorkerConfiguration config = WorkerConfiguration.builder()
                .withCQL(mockWorkerCQL)
                .withTransport(workerTransport)
                .withConsumer(noOpConsumer)
                .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
                .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
                .withClock(clock)
                .withCatchUpWindowSizeSeconds(catchUpSeconds)
                .build();

        // TEST_GENERATION is open (no end)
        GroupedTasks groupedTasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                TEST_GENERATION, Collections.singleton(TEST_TABLE_NAME));

        try (WorkerThread workerThread = new WorkerThread(config, groupedTasks)) {
            // Worker starts from generation start, same behavior as without catch-up
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                    TEST_GENERATION_START_MS, TEST_GENERATION_START_MS + DEFAULT_QUERY_WINDOW_SIZE_MS)));
        }
    }

    @Test
    public void testWorkerProbeSkippedWhenWindowRecent() {
        // Window start within catchUpWindowSizeSeconds of now → no probes.
        long genStartMs = 30 * 60 * 1000; // 30 minutes
        GenerationMetadata closedGeneration = MockGenerationMetadata.mockGenerationMetadata(
                new Timestamp(new Date(genStartMs)), Optional.of(new Timestamp(new Date(35 * 60 * 1000))),
                1, 1);

        // "Now" is at minute 32, catchUp = 3 minutes → cutoff at minute 29
        // Generation starts at minute 30, which is AFTER the cutoff, so no probes
        long nowMs = 32 * 60 * 1000;
        long catchUpSeconds = 3 * 60;
        Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.systemDefault());

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        WorkerConfiguration config = WorkerConfiguration.builder()
                .withCQL(mockWorkerCQL)
                .withTransport(workerTransport)
                .withConsumer(noOpConsumer)
                .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
                .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
                .withClock(clock)
                .withCatchUpWindowSizeSeconds(catchUpSeconds)
                .build();

        GroupedTasks groupedTasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                closedGeneration, Collections.singleton(TEST_TABLE_NAME));

        try (WorkerThread workerThread = new WorkerThread(config, groupedTasks)) {
            // Worker starts from generation start normally (no probing)
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(closedGeneration, 0, TEST_TABLE_NAME,
                    genStartMs, genStartMs + DEFAULT_QUERY_WINDOW_SIZE_MS)));
        }
    }

    @Test
    public void testWorkerProbeFallsBackWhenNoChanges() {
        // When probe returns Optional.empty() (no changes in stream),
        // the worker should fall back to the original window start.

        long genStartMs = 5 * 60 * 1000;
        long genEndMs = 10 * 60 * 1000;
        GenerationMetadata closedGeneration = MockGenerationMetadata.mockGenerationMetadata(
                new Timestamp(new Date(genStartMs)), Optional.of(new Timestamp(new Date(genEndMs))),
                1, 1);

        // "Now" is at minute 32, catchUp = 3 minutes → cutoff at minute 29
        long nowMs = 32 * 60 * 1000;
        long catchUpSeconds = 3 * 60;
        Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.systemDefault());

        // No raw changes → probe returns empty
        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        WorkerConfiguration config = WorkerConfiguration.builder()
                .withCQL(mockWorkerCQL)
                .withTransport(workerTransport)
                .withConsumer(noOpConsumer)
                .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
                .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
                .withClock(clock)
                .withCatchUpWindowSizeSeconds(catchUpSeconds)
                .build();

        GroupedTasks groupedTasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                closedGeneration, Collections.singleton(TEST_TABLE_NAME));

        try (WorkerThread workerThread = new WorkerThread(config, groupedTasks)) {
            // Probe was invoked but returned empty, so worker starts from generation start
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.getProbeInvocationCount() > 0);
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(closedGeneration, 0, TEST_TABLE_NAME,
                    genStartMs, genStartMs + DEFAULT_QUERY_WINDOW_SIZE_MS)));
        }
    }

    @Test
    public void testWorkerProbeExceptionFallsBack() {
        // When fetchFirstChangeTime throws an exception, the worker should
        // fall back to the original window start and continue reading normally.

        long genStartMs = 5 * 60 * 1000;
        long genEndMs = 10 * 60 * 1000;
        GenerationMetadata closedGeneration = MockGenerationMetadata.mockGenerationMetadata(
                new Timestamp(new Date(genStartMs)), Optional.of(new Timestamp(new Date(genEndMs))),
                1, 1);

        long nowMs = 32 * 60 * 1000;
        long catchUpSeconds = 3 * 60;
        Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.systemDefault());

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        mockWorkerCQL.setProbeFailureEnabled(true);
        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        WorkerConfiguration config = WorkerConfiguration.builder()
                .withCQL(mockWorkerCQL)
                .withTransport(workerTransport)
                .withConsumer(noOpConsumer)
                .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
                .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
                .withClock(clock)
                .withCatchUpWindowSizeSeconds(catchUpSeconds)
                .build();

        GroupedTasks groupedTasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                closedGeneration, Collections.singleton(TEST_TABLE_NAME));

        // The worker should gracefully handle the probe failure and continue
        // reading from the original window start.
        try (WorkerThread workerThread = new WorkerThread(config, groupedTasks)) {
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.getProbeInvocationCount() > 0);
            // Worker should continue running and start reading from the original window
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(closedGeneration, 0, TEST_TABLE_NAME,
                    genStartMs, genStartMs + DEFAULT_QUERY_WINDOW_SIZE_MS)));
        }
    }

    @Test
    public void testWorkerProbeMultiStreamFindsDataOnNonFirstStream() {
        // When a task has multiple streams and only a non-first stream has data,
        // all streams are probed and the task IS advanced to the earliest change found.

        long genStartMs = 5 * 60 * 1000;
        long genEndMs = 10 * 60 * 1000;
        // 1 vnode, 2 streams per vnode
        GenerationMetadata closedGeneration = MockGenerationMetadata.mockGenerationMetadata(
                new Timestamp(new Date(genStartMs)), Optional.of(new Timestamp(new Date(genEndMs))),
                1, 2);

        long nowMs = 32 * 60 * 1000;
        long catchUpSeconds = 3 * 60;
        Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.systemDefault());

        // Put a change on the SECOND stream
        Iterator<StreamId> streamIterator = closedGeneration.getStreams().iterator();
        streamIterator.next(); // skip first stream
        StreamId secondStream = streamIterator.next();

        long changeTimeMs = 7 * 60 * 1000;
        MockRawChange change = MockRawChange.builder()
                .withChangeSchema(TEST_CHANGE_SCHEMA)
                .withStreamId(secondStream.getValue())
                .withTimeMs(changeTimeMs)
                .addPrimaryKey("pk", 1)
                .addPrimaryKey("ck", 2)
                .addAtomicRegularColumn("v", 3)
                .build();

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        mockWorkerCQL.setRawChanges(Collections.singletonList(change));

        List<RawChange> observedChanges = Collections.synchronizedList(new ArrayList<>());
        Consumer accumulatingConsumer = Consumer.syncRawChangeConsumer(observedChanges::add);

        WorkerConfiguration config = WorkerConfiguration.builder()
                .withCQL(mockWorkerCQL)
                .withTransport(workerTransport)
                .withConsumer(accumulatingConsumer)
                .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
                .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
                .withClock(clock)
                .withCatchUpWindowSizeSeconds(catchUpSeconds)
                .build();

        GroupedTasks groupedTasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                closedGeneration, Collections.singleton(TEST_TABLE_NAME));

        try (WorkerThread workerThread = new WorkerThread(config, groupedTasks)) {
            // Both streams should be probed (2 streams)
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.getProbeInvocationCount() >= 2);

            // The worker should find the change (advanced to its timestamp)
            DEFAULT_AWAIT.until(() -> observedChanges.contains(change));

            // Verify the task was advanced to the change time (not starting from genStart)
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(closedGeneration, 0, TEST_TABLE_NAME,
                    changeTimeMs, changeTimeMs + DEFAULT_QUERY_WINDOW_SIZE_MS)));
        }
    }

    @Test
    public void testWorkerProbeSkippedForOpenGenerationAssertCount() {
        // Open generation → no probes even with catch-up enabled.
        // Verify via probe invocation counter.
        long nowMs = 32 * 60 * 1000;
        long catchUpSeconds = 3 * 60;
        Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.systemDefault());

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        WorkerConfiguration config = WorkerConfiguration.builder()
                .withCQL(mockWorkerCQL)
                .withTransport(workerTransport)
                .withConsumer(noOpConsumer)
                .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
                .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
                .withClock(clock)
                .withCatchUpWindowSizeSeconds(catchUpSeconds)
                .build();

        // TEST_GENERATION is open (no end)
        GroupedTasks groupedTasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                TEST_GENERATION, Collections.singleton(TEST_TABLE_NAME));

        try (WorkerThread workerThread = new WorkerThread(config, groupedTasks)) {
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                    TEST_GENERATION_START_MS, TEST_GENERATION_START_MS + DEFAULT_QUERY_WINDOW_SIZE_MS)));
            // No probes should have been invoked for an open generation
            assertEquals(0, mockWorkerCQL.getProbeInvocationCount());
        }
    }

    @Test
    public void testWorkerProbeSkippedWithExistingSavedState() {
        // When a task has existing saved state (i.e., resuming from a previous run),
        // the catch-up probe should be skipped even when catch-up is enabled.

        long genStartMs = 5 * 60 * 1000;
        long genEndMs = 10 * 60 * 1000;
        GenerationMetadata closedGeneration = MockGenerationMetadata.mockGenerationMetadata(
                new Timestamp(new Date(genStartMs)), Optional.of(new Timestamp(new Date(genEndMs))),
                1, 1);

        long nowMs = 32 * 60 * 1000;
        long catchUpSeconds = 3 * 60;
        Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.systemDefault());

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        // Pre-populate saved state for the task — simulating a resumed run.
        // The saved state starts at genStart + 2ms (window already advanced).
        long savedWindowStartMs = genStartMs + 2;
        long savedWindowEndMs = savedWindowStartMs + DEFAULT_QUERY_WINDOW_SIZE_MS;
        TaskId taskId = new TaskId(closedGeneration.getId(), new VNodeId(0), TEST_TABLE_NAME);
        TaskState savedState = new TaskState(new Timestamp(new Date(savedWindowStartMs)),
                new Timestamp(new Date(savedWindowEndMs)), Optional.empty());
        workerTransport.setState(taskId, savedState);

        WorkerConfiguration config = WorkerConfiguration.builder()
                .withCQL(mockWorkerCQL)
                .withTransport(workerTransport)
                .withConsumer(noOpConsumer)
                .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
                .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
                .withClock(clock)
                .withCatchUpWindowSizeSeconds(catchUpSeconds)
                .build();

        GroupedTasks groupedTasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                closedGeneration, Collections.singleton(TEST_TABLE_NAME));

        try (WorkerThread workerThread = new WorkerThread(config, groupedTasks)) {
            // Worker should start reading from the saved state window
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(closedGeneration, 0, TEST_TABLE_NAME,
                    savedWindowStartMs, savedWindowEndMs)));

            // Probe should NOT have been called since the task has saved state
            assertEquals(0, mockWorkerCQL.getProbeInvocationCount());
        }
    }

    @Test
    public void testWorkerProbeMixedSavedState() {
        // Multiple vnodes: some with saved state (should skip probe) and
        // some without (should probe). Verifies per-task filtering logic.

        long genStartMs = 5 * 60 * 1000;
        long genEndMs = 10 * 60 * 1000;
        // 2 vnodes, 1 stream each
        GenerationMetadata closedGeneration = MockGenerationMetadata.mockGenerationMetadata(
                new Timestamp(new Date(genStartMs)), Optional.of(new Timestamp(new Date(genEndMs))),
                2, 1);

        long nowMs = 32 * 60 * 1000;
        long catchUpSeconds = 3 * 60;
        Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.systemDefault());

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        // Pre-populate saved state for vnode 0 only
        TaskId savedTaskId = new TaskId(closedGeneration.getId(), new VNodeId(0), TEST_TABLE_NAME);
        long savedWindowStartMs = genStartMs + 2;
        long savedWindowEndMs = savedWindowStartMs + DEFAULT_QUERY_WINDOW_SIZE_MS;
        TaskState savedState = new TaskState(new Timestamp(new Date(savedWindowStartMs)),
                new Timestamp(new Date(savedWindowEndMs)), Optional.empty());
        workerTransport.setState(savedTaskId, savedState);

        // vnode 1 has NO saved state → should be probed

        WorkerConfiguration config = WorkerConfiguration.builder()
                .withCQL(mockWorkerCQL)
                .withTransport(workerTransport)
                .withConsumer(noOpConsumer)
                .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
                .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
                .withClock(clock)
                .withCatchUpWindowSizeSeconds(catchUpSeconds)
                .build();

        GroupedTasks groupedTasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                closedGeneration, Collections.singleton(TEST_TABLE_NAME));

        try (WorkerThread workerThread = new WorkerThread(config, groupedTasks)) {
            // vnode 0 should read from saved state
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(closedGeneration, 0, TEST_TABLE_NAME,
                    savedWindowStartMs, savedWindowEndMs)));

            // Only 1 probe should have fired (for vnode 1 which has no saved state)
            assertEquals(1, mockWorkerCQL.getProbeInvocationCount());
        }
    }

    @Test
    public void testWorkerProbeWithMultipleTables() {
        // When catch-up is enabled with multiple tables, probes should fire
        // for each table's tasks independently.

        long genStartMs = 5 * 60 * 1000;
        long genEndMs = 10 * 60 * 1000;
        // 2 vnodes, 1 stream each — one vnode per table effectively
        GenerationMetadata closedGeneration = MockGenerationMetadata.mockGenerationMetadata(
                new Timestamp(new Date(genStartMs)), Optional.of(new Timestamp(new Date(genEndMs))),
                2, 1);

        long nowMs = 32 * 60 * 1000;
        long catchUpSeconds = 3 * 60;
        Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.systemDefault());

        TableName table1 = new TableName("ks", "t1");
        TableName table2 = new TableName("ks", "t2");
        Set<TableName> tables = new HashSet<>();
        tables.add(table1);
        tables.add(table2);

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        WorkerConfiguration config = WorkerConfiguration.builder()
                .withCQL(mockWorkerCQL)
                .withTransport(workerTransport)
                .withConsumer(noOpConsumer)
                .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
                .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
                .withClock(clock)
                .withCatchUpWindowSizeSeconds(catchUpSeconds)
                .build();

        GroupedTasks groupedTasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                closedGeneration, tables);

        try (WorkerThread workerThread = new WorkerThread(config, groupedTasks)) {
            // With 2 vnodes * 2 tables = 4 tasks, each with 1 stream = 4 probes
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.getProbeInvocationCount() >= 4);

            // Both tables' tasks should start reading
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(closedGeneration, 0, table1,
                    genStartMs, genStartMs + DEFAULT_QUERY_WINDOW_SIZE_MS)));
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(closedGeneration, 0, table2,
                    genStartMs, genStartMs + DEFAULT_QUERY_WINDOW_SIZE_MS)));
        }
    }

    @Test
    public void testWorkerProbeClampedToGenerationStart() {
        // When the probe returns a timestamp BEFORE the generation start,
        // it should be clamped to the generation start.

        long genStartMs = 5 * 60 * 1000; // 5 minutes
        long genEndMs = 10 * 60 * 1000;  // 10 minutes
        GenerationMetadata closedGeneration = MockGenerationMetadata.mockGenerationMetadata(
                new Timestamp(new Date(genStartMs)), Optional.of(new Timestamp(new Date(genEndMs))),
                1, 1);

        long nowMs = 32 * 60 * 1000;
        long catchUpSeconds = 3 * 60;
        Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.systemDefault());

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        // Override probe to return a timestamp before generation start (clock skew scenario)
        mockWorkerCQL.setProbeResultOverride(Optional.of(new Timestamp(new Date(1000))));
        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        WorkerConfiguration config = WorkerConfiguration.builder()
                .withCQL(mockWorkerCQL)
                .withTransport(workerTransport)
                .withConsumer(noOpConsumer)
                .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
                .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
                .withClock(clock)
                .withCatchUpWindowSizeSeconds(catchUpSeconds)
                .build();

        GroupedTasks groupedTasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                closedGeneration, Collections.singleton(TEST_TABLE_NAME));

        try (WorkerThread workerThread = new WorkerThread(config, groupedTasks)) {
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.getProbeInvocationCount() > 0);
            // The probe result (1000ms) is before genStart (5min), so it should be clamped.
            // Since genStart equals the original window start, the task should NOT be advanced
            // (clamped value is not ahead of window start).
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(closedGeneration, 0, TEST_TABLE_NAME,
                    genStartMs, genStartMs + DEFAULT_QUERY_WINDOW_SIZE_MS)));
        }
    }

    @Test
    public void testWorkerProbeClampedToGenerationEnd() {
        // When the probe returns a timestamp AFTER generationEnd - 1ms,
        // it should be clamped to generationEnd - 1ms.

        long genStartMs = 5 * 60 * 1000;  // 5 minutes
        long genEndMs = 10 * 60 * 1000;   // 10 minutes
        GenerationMetadata closedGeneration = MockGenerationMetadata.mockGenerationMetadata(
                new Timestamp(new Date(genStartMs)), Optional.of(new Timestamp(new Date(genEndMs))),
                1, 1);

        long nowMs = 32 * 60 * 1000;
        long catchUpSeconds = 3 * 60;
        Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.systemDefault());

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        // Override probe to return a timestamp after generation end
        mockWorkerCQL.setProbeResultOverride(Optional.of(new Timestamp(new Date(genEndMs + 5000))));
        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        WorkerConfiguration config = WorkerConfiguration.builder()
                .withCQL(mockWorkerCQL)
                .withTransport(workerTransport)
                .withConsumer(noOpConsumer)
                .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
                .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
                .withClock(clock)
                .withCatchUpWindowSizeSeconds(catchUpSeconds)
                .build();

        GroupedTasks groupedTasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                closedGeneration, Collections.singleton(TEST_TABLE_NAME));

        try (WorkerThread workerThread = new WorkerThread(config, groupedTasks)) {
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.getProbeInvocationCount() > 0);
            // Probe result is after genEnd, so clamped to genEnd - 1ms
            long clampedMs = genEndMs - 1;
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(closedGeneration, 0, TEST_TABLE_NAME,
                    clampedMs, clampedMs + DEFAULT_QUERY_WINDOW_SIZE_MS)));
        }
    }

    @Test
    public void testWorkerProbeConcurrencyBoundedBySemaphore() {
        // Create a generation with many vnodes (more than MAX_CONCURRENT_PROBES = 64).
        // Each vnode has 1 stream, so total probes = number of vnodes.
        // Use a delay on probes so they overlap and the semaphore actually bounds them.
        int vnodeCount = 80;
        long genStartMs = 5 * 60 * 1000;
        long genEndMs = 10 * 60 * 1000;
        GenerationMetadata closedGeneration = MockGenerationMetadata.mockGenerationMetadata(
                new Timestamp(new Date(genStartMs)), Optional.of(new Timestamp(new Date(genEndMs))),
                vnodeCount, 1);

        long nowMs = 32 * 60 * 1000;
        long catchUpSeconds = 3 * 60;
        Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.systemDefault());

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        mockWorkerCQL.setProbeResultOverride(Optional.empty());
        mockWorkerCQL.setProbeDelayMs(50);

        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        WorkerConfiguration config = WorkerConfiguration.builder()
                .withCQL(mockWorkerCQL)
                .withTransport(workerTransport)
                .withConsumer(noOpConsumer)
                .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
                .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
                .withClock(clock)
                .withCatchUpWindowSizeSeconds(catchUpSeconds)
                .build();

        GroupedTasks groupedTasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                closedGeneration, Collections.singleton(TEST_TABLE_NAME));

        try (WorkerThread workerThread = new WorkerThread(config, groupedTasks)) {
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.getProbeInvocationCount() >= vnodeCount);
            // The semaphore should have bounded concurrent probes to MAX_CONCURRENT_PROBES
            assertTrue(mockWorkerCQL.getPeakConcurrentProbes() <= CatchUpProber.MAX_CONCURRENT_PROBES,
                    "Peak concurrent probes " + mockWorkerCQL.getPeakConcurrentProbes()
                            + " exceeded limit " + CatchUpProber.MAX_CONCURRENT_PROBES);
        }
    }

    @Test
    public void testWorkerProbeInterruptCancelsFutures() throws Exception {
        // When the worker thread is interrupted during probe result collection,
        // remaining futures should be cancelled.
        long genStartMs = 5 * 60 * 1000;
        long genEndMs = 10 * 60 * 1000;
        GenerationMetadata closedGeneration = MockGenerationMetadata.mockGenerationMetadata(
                new Timestamp(new Date(genStartMs)), Optional.of(new Timestamp(new Date(genEndMs))),
                4, 1);

        long nowMs = 32 * 60 * 1000;
        long catchUpSeconds = 3 * 60;
        Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.systemDefault());

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        // Use a long delay so probes are still in-flight when we interrupt
        mockWorkerCQL.setProbeResultOverride(Optional.empty());
        mockWorkerCQL.setProbeDelayMs(5000);

        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        WorkerConfiguration config = WorkerConfiguration.builder()
                .withCQL(mockWorkerCQL)
                .withTransport(workerTransport)
                .withConsumer(noOpConsumer)
                .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
                .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
                .withClock(clock)
                .withCatchUpWindowSizeSeconds(catchUpSeconds)
                .build();

        GroupedTasks groupedTasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                closedGeneration, Collections.singleton(TEST_TABLE_NAME));

        Worker worker = new Worker(config);
        Thread workerThread = new Thread(() -> {
            try {
                worker.run(groupedTasks);
            } catch (Exception e) {
                // Expected — interrupt during probe
            }
        });
        workerThread.start();

        // Wait for probes to start
        DEFAULT_AWAIT.until(() -> mockWorkerCQL.getProbeInvocationCount() > 0);

        // Interrupt the worker thread
        workerThread.interrupt();

        // Worker thread should finish quickly after interrupt
        workerThread.join(5000);
        assertFalse(workerThread.isAlive(), "Worker thread should have stopped after interrupt");
    }

    @Test
    public void testWorkerProbeTimeoutFallsBack() {
        // When a probe takes longer than probeTimeoutSeconds, the worker should
        // fall back to the original window start (TimeoutException path).

        long genStartMs = 5 * 60 * 1000;
        long genEndMs = 10 * 60 * 1000;
        GenerationMetadata closedGeneration = MockGenerationMetadata.mockGenerationMetadata(
                new Timestamp(new Date(genStartMs)), Optional.of(new Timestamp(new Date(genEndMs))),
                1, 1);

        long nowMs = 32 * 60 * 1000;
        long catchUpSeconds = 3 * 60;
        Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.systemDefault());

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        // Probe takes 10 seconds but timeout is 1 second
        mockWorkerCQL.setProbeResultOverride(Optional.of(new Timestamp(new Date(7 * 60 * 1000))));
        mockWorkerCQL.setProbeDelayMs(10_000);

        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        WorkerConfiguration config = WorkerConfiguration.builder()
                .withCQL(mockWorkerCQL)
                .withTransport(workerTransport)
                .withConsumer(noOpConsumer)
                .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
                .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
                .withClock(clock)
                .withCatchUpWindowSizeSeconds(catchUpSeconds)
                .withProbeTimeoutSeconds(1)
                .build();

        GroupedTasks groupedTasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                closedGeneration, Collections.singleton(TEST_TABLE_NAME));

        try (WorkerThread workerThread = new WorkerThread(config, groupedTasks)) {
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.getProbeInvocationCount() > 0);
            // Probe timed out, so worker should fall back to original window start
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(closedGeneration, 0, TEST_TABLE_NAME,
                    genStartMs, genStartMs + DEFAULT_QUERY_WINDOW_SIZE_MS)));
        }
    }

    @Test
    public void testWorkerCatchUpWithSmallTTLStillFunctions() {
        // When the table TTL is smaller than the catch-up window, the worker should
        // log a warning but still function correctly.
        // Generation: 5min - 10min, now: 8min, TTL: 120s (2min), catch-up: 5min
        // TTL trims window start to max(genStart, now-ttl) = max(5min, 6min) = 6min
        // Cutoff = now - catchUp = 8min - 5min = 3min. Window start (6min) > cutoff (3min), so no probing.
        long genStartMs = 5 * 60 * 1000;
        long genEndMs = 10 * 60 * 1000;
        GenerationMetadata closedGeneration = MockGenerationMetadata.mockGenerationMetadata(
                new Timestamp(new Date(genStartMs)), Optional.of(new Timestamp(new Date(genEndMs))),
                1, 1);

        long nowMs = 8 * 60 * 1000;    // 8 minutes
        long catchUpSeconds = 5 * 60;   // 5 minutes
        long ttlSeconds = 120;           // 2 minutes (smaller than catch-up)
        Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.systemDefault());

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        mockWorkerCQL.setTablesTTL(Collections.singletonMap(TEST_TABLE_NAME, Optional.of(ttlSeconds)));
        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        WorkerConfiguration config = WorkerConfiguration.builder()
                .withCQL(mockWorkerCQL)
                .withTransport(workerTransport)
                .withConsumer(noOpConsumer)
                .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
                .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
                .withClock(clock)
                .withCatchUpWindowSizeSeconds(catchUpSeconds)
                .build();

        GroupedTasks groupedTasks = MockGenerationMetadata.generationMetadataToWorkerTasks(
                closedGeneration, Collections.singleton(TEST_TABLE_NAME));

        try (WorkerThread workerThread = new WorkerThread(config, groupedTasks)) {
            // TTL trimming moves window start to 6min. Cutoff is 3min.
            // Since window start (6min) is after cutoff (3min), no probing needed.
            // Worker should start reading from the TTL-trimmed position.
            long ttlTrimmedMs = nowMs - ttlSeconds * 1000; // 6 minutes
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(closedGeneration, 0, TEST_TABLE_NAME,
                    ttlTrimmedMs, ttlTrimmedMs + DEFAULT_QUERY_WINDOW_SIZE_MS)));
            // No probes needed since the trimmed window is recent enough
            assertEquals(0, mockWorkerCQL.getProbeInvocationCount());
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
