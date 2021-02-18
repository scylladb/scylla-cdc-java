package com.scylladb.cdc.model.master;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.scylladb.cdc.cql.MockMasterCQL;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.transport.ConfigureWorkersTracker;
import com.scylladb.cdc.transport.MockMasterTransport;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.scylladb.cdc.model.master.MockGenerationMetadata.mockGenerationMetadata;
import static org.awaitility.Awaitility.with;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MasterTest {
    private static final long DEFAULT_AWAIT_TIMEOUT_MS = 2000;
    private static final ConditionFactory DEFAULT_AWAIT =
            with().pollInterval(1, TimeUnit.MILLISECONDS).await()
                    .atMost(DEFAULT_AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    // Test generations. To make life easier, test set with X+1 generations
    // should start with the same generations as a test set with X generations
    // (and a new one).
    private static final List<GenerationMetadata> TEST_SET_ONE_GENERATION = Lists.newArrayList(
            mockGenerationMetadata(mockTimestamp(5), Optional.empty(), 8)
    );

    private static final List<GenerationMetadata> TEST_SET_TWO_GENERATIONS = Lists.newArrayList(
            mockGenerationMetadata(mockTimestamp(5), Optional.of(mockTimestamp(10)), 8),
            mockGenerationMetadata(mockTimestamp(10), Optional.empty(), 8)
    );

    private static final List<GenerationMetadata> TEST_SET_THREE_GENERATIONS = Lists.newArrayList(
            mockGenerationMetadata(mockTimestamp(5), Optional.of(mockTimestamp(10)), 8),
            mockGenerationMetadata(mockTimestamp(10), Optional.of(mockTimestamp(30)), 8),
            mockGenerationMetadata(mockTimestamp(30), Optional.empty(), 10)
    );

    private static final Set<TableName> TEST_SET_SINGLE_TABLE = Collections.singleton(
            new TableName("ks", "test")
    );

    private static final Set<TableName> TEST_SET_TWO_TABLES = Sets.newHashSet(
            new TableName("ks", "test"),
            new TableName("ks", "test2")
    );

    @Test
    public void testMasterConfiguresOneGeneration() {
        // MasterTransport without an initial generation
        // (starting from beginning) and never finishing
        // any generation.
        MockMasterTransport masterTransport = new MockMasterTransport();
        ConfigureWorkersTracker masterTransportTracker = masterTransport.tracker(DEFAULT_AWAIT);

        MockMasterCQL masterCQL = new MockMasterCQL(TEST_SET_TWO_GENERATIONS);
        Set<TableName> tableNames = TEST_SET_SINGLE_TABLE;

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames)) {
            // Verify that the transport received the first generation...
            masterTransportTracker.awaitConfigureWorkers(TEST_SET_TWO_GENERATIONS.get(0), tableNames);

            // ...and it checked multiple times if it should move to the
            //    next generation...
            awaitAreTasksFullyConsumedUntilInvocations(masterTransport);
        }

        // ...but ultimately it didn't go to the next generation.
        masterTransportTracker.checkNoAdditionalConfigureWorkers();
    }

    @Test
    public void testMasterConfiguresTwoGenerations() {
        MockMasterTransport masterTransport = new MockMasterTransport();
        ConfigureWorkersTracker masterTransportTracker = masterTransport.tracker(DEFAULT_AWAIT);

        MockMasterCQL masterCQL = new MockMasterCQL(TEST_SET_TWO_GENERATIONS);
        Set<TableName> tableNames = TEST_SET_SINGLE_TABLE;

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames)) {
            // Verify that the transport received the first generation...
            masterTransportTracker.awaitConfigureWorkers(TEST_SET_TWO_GENERATIONS.get(0), tableNames);

            // ...and it checked multiple times if it should move to the
            //    next generation, but did not do it...
            awaitAreTasksFullyConsumedUntilInvocations(masterTransport);
            masterTransportTracker.checkNoAdditionalConfigureWorkers();

            // ...until the first generation was fully consumed and
            //    only then the move occurred.
            masterTransport.setGenerationFullyConsumed(TEST_SET_TWO_GENERATIONS.get(0));
            masterTransportTracker.awaitConfigureWorkers(TEST_SET_TWO_GENERATIONS.get(1), tableNames);
        }
    }

    @Test
    public void testMasterDiscoversNewGeneration() {
        MockMasterTransport masterTransport = new MockMasterTransport();
        ConfigureWorkersTracker masterTransportTracker = masterTransport.tracker(DEFAULT_AWAIT);

        // Start with only a single generation visible.
        MockMasterCQL masterCQL = new MockMasterCQL(TEST_SET_ONE_GENERATION);
        Set<TableName> tableNames = TEST_SET_SINGLE_TABLE;

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames)) {
            // Verify that the transport received the first generation...
            masterTransportTracker.awaitConfigureWorkers(TEST_SET_ONE_GENERATION.get(0), tableNames);

            // ...and it performed multiple CQL queries...
            awaitCQLInvocations(masterCQL);

            // ...but because it was not closed, it did not check if tasks were fully consumed.
            assertEquals(masterTransport.getAreTasksFullyConsumedUntilCount(), 0);

            // New generation appears...
            masterCQL.setGenerationMetadatas(TEST_SET_TWO_GENERATIONS);

            // ...now we check if we should move to the next generation - not yet!...
            awaitAreTasksFullyConsumedUntilInvocations(masterTransport);
            masterTransportTracker.checkNoAdditionalConfigureWorkers();

            // ...make current generation fully consumed...
            masterTransport.setGenerationFullyConsumed(TEST_SET_TWO_GENERATIONS.get(0));

            // ...and observe moving to the next one.
            masterTransportTracker.awaitConfigureWorkers(TEST_SET_TWO_GENERATIONS.get(1), tableNames);
        }
    }

    @Test
    public void testMasterResumesFromCurrentGeneration() {
        // MasterTransport with specified current generation
        // and never finishing any generation.
        MockMasterTransport masterTransport = new MockMasterTransport();
        masterTransport.setCurrentGenerationId(Optional.of(TEST_SET_TWO_GENERATIONS.get(1).getId()));
        ConfigureWorkersTracker masterTransportTracker = masterTransport.tracker(DEFAULT_AWAIT);

        MockMasterCQL masterCQL = new MockMasterCQL(TEST_SET_TWO_GENERATIONS);
        Set<TableName> tableNames = TEST_SET_SINGLE_TABLE;

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames)) {
            // Verify that the transport received the second generation.
            masterTransportTracker.awaitConfigureWorkers(TEST_SET_TWO_GENERATIONS.get(1), tableNames);
        }
    }

    @Test
    public void testMasterWaitsForFirstGeneration() {
        // MasterTransport without an initial generation
        // (starting from beginning) and never finishing
        // any generation.
        MockMasterTransport masterTransport = new MockMasterTransport();
        ConfigureWorkersTracker masterTransportTracker = masterTransport.tracker(DEFAULT_AWAIT);

        MockMasterCQL masterCQL = new MockMasterCQL();
        Set<TableName> tableNames = TEST_SET_SINGLE_TABLE;

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames)) {
            // Verify that the master tries to find the first generation.
            awaitCQLInvocations(masterCQL);
            masterTransportTracker.checkNoAdditionalConfigureWorkers();

            // First generation appears...
            masterCQL.setGenerationMetadatas(TEST_SET_ONE_GENERATION);

            // ...and then the transport receives it.
            masterTransportTracker.awaitConfigureWorkers(TEST_SET_ONE_GENERATION.get(0), tableNames);
        }
    }

    @Test
    public void testMasterResilientToCQLExceptions() {
        MockMasterTransport masterTransport = new MockMasterTransport();
        ConfigureWorkersTracker masterTransportTracker = masterTransport.tracker(DEFAULT_AWAIT);

        MockMasterCQL masterCQL = new MockMasterCQL(TEST_SET_TWO_GENERATIONS);
        Set<TableName> tableNames = TEST_SET_SINGLE_TABLE;

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames)) {
            // Verify that the transport received the first generation.
            masterTransportTracker.awaitConfigureWorkers(TEST_SET_TWO_GENERATIONS.get(0), tableNames);

            // We simulate constant CQL failure...
            masterCQL.setShouldInjectFailure(true);

            // ...new generation appears and current is fully consumed...
            masterCQL.setGenerationMetadatas(TEST_SET_TWO_GENERATIONS);
            masterTransport.setGenerationFullyConsumed(TEST_SET_TWO_GENERATIONS.get(0));

            // ...CQL queries are performed, but they fail and no new generation is discovered...
            awaitCQLInvocations(masterCQL);
            masterTransportTracker.checkNoAdditionalConfigureWorkers();

            // ...the CQL is back working...
            masterCQL.setShouldInjectFailure(false);

            // ...and observe moving to the next generation.
            masterTransportTracker.awaitConfigureWorkers(TEST_SET_TWO_GENERATIONS.get(1), tableNames);
        }
    }

    @Test
    public void testMasterSkipsGenerationsDueToSingleTableTTL() {
        MockMasterTransport masterTransport = new MockMasterTransport();
        ConfigureWorkersTracker masterTransportTracker = masterTransport.tracker(DEFAULT_AWAIT);

        MockMasterCQL masterCQL = new MockMasterCQL(TEST_SET_THREE_GENERATIONS);
        Set<TableName> tableNames = TEST_SET_SINGLE_TABLE;

        // The generations are as follows (in minutes after epoch):
        //  5 - 10
        // 10 - 30
        // 30 - infinity

        Clock simulatedTime = Clock.fixed(mockTimestamp(32).toDate().toInstant(), ZoneOffset.systemDefault());

        // First test.
        //
        // We start at minute 32 with TTL of 3 minutes (provided below in seconds).
        // We should catch 2 generations.
        masterCQL.setTablesTTL(Collections.singletonMap(tableNames.iterator().next(), Optional.of(3L * 60)));

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames, simulatedTime)) {
            // Verify that the transport received the second generation...
            masterTransportTracker.awaitConfigureWorkers(TEST_SET_THREE_GENERATIONS.get(1), tableNames);

            // ...make the second generation fully consumed...
            masterTransport.setGenerationFullyConsumed(TEST_SET_THREE_GENERATIONS.get(1));

            // ...and we should switch to the third generation...
            masterTransportTracker.awaitConfigureWorkers(TEST_SET_THREE_GENERATIONS.get(2), tableNames);
        }

        // Second test.
        //
        // We start at minute 32 with TTL of 1 minute.
        // We should catch only the last generation.
        masterCQL.setTablesTTL(Collections.singletonMap(tableNames.iterator().next(), Optional.of(1L * 60)));

        // Reset setGenerationFullyConsumed calls.
        masterTransport.setCurrentFullyConsumedTimestamp(new Timestamp(new Date(0)));

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames, simulatedTime)) {
            // Verify that the transport received the third generation...
            masterTransportTracker.awaitConfigureWorkers(TEST_SET_THREE_GENERATIONS.get(2), tableNames);
        }

        // Third test
        //
        // We start at minute 32 with TTL of 24 minutes.
        // We should catch all generations.
        masterCQL.setTablesTTL(Collections.singletonMap(tableNames.iterator().next(), Optional.of(24L * 60)));

        // Reset setGenerationFullyConsumed calls.
        masterTransport.setCurrentFullyConsumedTimestamp(new Timestamp(new Date(0)));

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames, simulatedTime)) {
            for (GenerationMetadata generation : TEST_SET_THREE_GENERATIONS) {
                masterTransportTracker.awaitConfigureWorkers(generation, tableNames);
                if (generation.isClosed()) {
                    masterTransport.setGenerationFullyConsumed(generation);
                }
            }
        }

        // Fourth test
        //
        // We start at minute 32 with no TTL.
        // We should catch all generations.
        masterCQL.setTablesTTL(Collections.singletonMap(tableNames.iterator().next(), Optional.empty()));

        // Reset setGenerationFullyConsumed calls.
        masterTransport.setCurrentFullyConsumedTimestamp(new Timestamp(new Date(0)));

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames, simulatedTime)) {
            for (GenerationMetadata generation : TEST_SET_THREE_GENERATIONS) {
                masterTransportTracker.awaitConfigureWorkers(generation, tableNames);
                if (generation.isClosed()) {
                    masterTransport.setGenerationFullyConsumed(generation);
                }
            }
        }
    }

    @Test
    public void testMasterSkipsGenerationsDueToMultipleTablesTTL() {
        MockMasterTransport masterTransport = new MockMasterTransport();
        ConfigureWorkersTracker masterTransportTracker = masterTransport.tracker(DEFAULT_AWAIT);

        MockMasterCQL masterCQL = new MockMasterCQL(TEST_SET_THREE_GENERATIONS);
        Set<TableName> tableNames = TEST_SET_TWO_TABLES;

        // The generations are as follows (in minutes after epoch):
        //  5 - 10
        // 10 - 30
        // 30 - infinity

        Clock simulatedTime = Clock.fixed(mockTimestamp(32).toDate().toInstant(), ZoneOffset.systemDefault());

        // First test.
        //
        // We start at minute 32 with TTL of 1 minute for first table and 6 minutes for second table.
        // We should catch 2 generations.
        masterCQL.setTablesTTL(new HashMap<TableName, Optional<Long>>() {{
            Iterator<TableName> iterator = tableNames.iterator();
            put(iterator.next(), Optional.of(1L * 60));
            put(iterator.next(), Optional.of(6L * 60));
        }});

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames, simulatedTime)) {
            masterTransportTracker.awaitConfigureWorkers(TEST_SET_THREE_GENERATIONS.get(1), tableNames);
            masterTransport.setGenerationFullyConsumed(TEST_SET_THREE_GENERATIONS.get(1));

            masterTransportTracker.awaitConfigureWorkers(TEST_SET_THREE_GENERATIONS.get(2), tableNames);
        }

        // Second test.
        //
        // We start at minute 32 with TTL of 1 minute for first table and disabled TTL for second table.
        // We should catch all generations.
        masterCQL.setTablesTTL(new HashMap<TableName, Optional<Long>>() {{
            Iterator<TableName> iterator = tableNames.iterator();
            put(iterator.next(), Optional.of(1L * 60));
            put(iterator.next(), Optional.empty());
        }});

        // Reset setGenerationFullyConsumed calls.
        masterTransport.setCurrentFullyConsumedTimestamp(new Timestamp(new Date(0)));

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames, simulatedTime)) {
            for (GenerationMetadata generation : TEST_SET_THREE_GENERATIONS) {
                masterTransportTracker.awaitConfigureWorkers(generation, tableNames);
                if (generation.isClosed()) {
                    masterTransport.setGenerationFullyConsumed(generation);
                }
            }
        }
    }

    private static Timestamp mockTimestamp(long minutesAfterEpoch) {
        // Minutes to milliseconds:
        return new Timestamp(new Date(minutesAfterEpoch * 60 * 1000));
    }

    private void awaitInvocationIncrease(Supplier<Integer> invocationCount, int awaitingCount) {
        int invocationsBefore = invocationCount.get();
        int invocationsAfter = invocationsBefore + awaitingCount;
        DEFAULT_AWAIT.until(() -> invocationCount.get() >= invocationsAfter);
    }

    private void awaitCQLInvocations(MockMasterCQL masterCQL) {
        awaitCQLInvocations(masterCQL, 10);
    }

    private void awaitCQLInvocations(MockMasterCQL masterCQL, int count) {
        awaitInvocationIncrease(masterCQL::getFetchCount, count);
    }

    private void awaitAreTasksFullyConsumedUntilInvocations(MockMasterTransport transport) {
        awaitAreTasksFullyConsumedUntilInvocations(transport, 10);
    }

    private void awaitAreTasksFullyConsumedUntilInvocations(MockMasterTransport transport, int count) {
        awaitInvocationIncrease(transport::getAreTasksFullyConsumedUntilCount, count);
    }
}
