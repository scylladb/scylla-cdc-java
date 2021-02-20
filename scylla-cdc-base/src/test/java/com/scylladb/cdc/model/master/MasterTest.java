package com.scylladb.cdc.model.master;

import com.google.common.collect.Lists;
import com.scylladb.cdc.cql.MockMasterCQL;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.transport.ConfigureWorkersTracker;
import com.scylladb.cdc.transport.MockMasterTransport;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.awaitility.Awaitility.with;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MasterTest {
    private static final long DEFAULT_AWAIT_TIMEOUT_MS = 2000;
    private static final ConditionFactory DEFAULT_AWAIT =
            with().pollInterval(1, TimeUnit.MILLISECONDS).await()
                    .atMost(DEFAULT_AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    private static final List<GenerationMetadata> TEST_SET_ONE_GENERATION = Lists.newArrayList(
            MockGenerationMetadata.mockGenerationMetadata(5, Optional.empty(), 8)
    );

    private static final List<GenerationMetadata> TEST_SET_TWO_GENERATIONS = Lists.newArrayList(
            MockGenerationMetadata.mockGenerationMetadata(5, Optional.of(10L), 8),
            MockGenerationMetadata.mockGenerationMetadata(10, Optional.empty(), 8)
    );

    private static final Set<TableName> TEST_SET_SINGLE_TABLE = Collections.singleton(
            new TableName("ks", "test")
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
