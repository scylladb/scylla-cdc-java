package com.scylladb.cdc.model.master;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.scylladb.cdc.cql.MockMasterCQL;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
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

    // Table-specific generations for tablets mode tests
    private static final Map<TableName, List<GenerationMetadata>> TEST_TABLE_GENERATIONS = Maps.newHashMap(Map.of(
            new TableName("ks", "test"), Lists.newArrayList(mockGenerationMetadata(mockTimestamp(5), Optional.empty(), 1, 1)),
            new TableName("ks", "test2"), Lists.newArrayList(mockGenerationMetadata(mockTimestamp(7), Optional.empty(), 1, 1))
    ));

    // Single table tablet mode test generations
    private static final Map<TableName, List<GenerationMetadata>> TEST_SINGLE_TABLE_TABLET_GENERATIONS = Maps.newHashMap(Map.of(
            new TableName("ks", "test"), Lists.newArrayList(mockGenerationMetadata(mockTimestamp(5), Optional.empty(), 4, 1))
    ));

    // Add test data for multiple tablet generations
    private static final Map<TableName, List<GenerationMetadata>> TEST_TABLE_MULTIPLE_GENERATIONS = Maps.newHashMap(Map.of(
            new TableName("ks", "test"), Lists.newArrayList(
                    mockGenerationMetadata(mockTimestamp(5), Optional.of(mockTimestamp(15)), 4, 1),
                    mockGenerationMetadata(mockTimestamp(15), Optional.empty(), 4, 1)
            )
    ));

    // Test data for multiple tables with multiple generations each
    private static final Map<TableName, List<GenerationMetadata>> TEST_TABLES_MULTIPLE_GENERATIONS = Maps.newHashMap(Map.of(
            new TableName("ks", "test1"), Lists.newArrayList(
                    mockGenerationMetadata(mockTimestamp(5), Optional.of(mockTimestamp(15)), 4, 1),
                    mockGenerationMetadata(mockTimestamp(15), Optional.empty(), 4, 1)
            ),
            new TableName("ks", "test2"), Lists.newArrayList(
                    mockGenerationMetadata(mockTimestamp(6), Optional.of(mockTimestamp(16)), 4, 1),
                    mockGenerationMetadata(mockTimestamp(16), Optional.empty(), 4, 1)
            )
    ));

    // Test data for tablet mode with three generations for TTL testing
    private static final Map<TableName, List<GenerationMetadata>> TEST_TABLE_THREE_GENERATIONS = Maps.newHashMap(Map.of(
            new TableName("ks", "test"), Lists.newArrayList(
                    mockGenerationMetadata(mockTimestamp(5), Optional.of(mockTimestamp(10)), 4, 1),
                    mockGenerationMetadata(mockTimestamp(10), Optional.of(mockTimestamp(30)), 4, 1),
                    mockGenerationMetadata(mockTimestamp(30), Optional.empty(), 4, 1)
            )
    ));

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

    @Test
    public void testMasterConfiguresOneGenerationTabletMode() {
        // Test with tablet-based CDC in single generation mode (simplified version)
        MockMasterTransport masterTransport = new MockMasterTransport();
        ConfigureWorkersTracker masterTransportTracker = masterTransport.tracker(DEFAULT_AWAIT);

        // Create CQL mock with tablets mode enabled
        MockMasterCQL masterCQL = new MockMasterCQL();
        masterCQL.setUsesTablets(true);

        // Use only a single table
        Set<TableName> tableNames = TEST_SET_SINGLE_TABLE;
        TableName testTable = tableNames.iterator().next();

        // Use the statically defined table generations
        masterCQL.setTableGenerationMetadatas(TEST_SINGLE_TABLE_TABLET_GENERATIONS);

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames)) {
            // Wait for the table's generation to be configured using the per-table method
            GenerationMetadata expectedGeneration = TEST_SINGLE_TABLE_TABLET_GENERATIONS.get(testTable).get(0);
            masterTransportTracker.awaitConfigureWorkers(testTable, expectedGeneration);
        }

        // Verify that it didn't configure any additional generations specifically for this table
        masterTransportTracker.checkNoAdditionalConfigureWorkers(testTable);
    }

    @Test
    public void testMasterConfiguresTwoTablesTabletMode() {
        // Test with tablet-based CDC in single generation mode for two tables
        MockMasterTransport masterTransport = new MockMasterTransport();
        ConfigureWorkersTracker masterTransportTracker = masterTransport.tracker(DEFAULT_AWAIT);

        // Create CQL mock with tablets mode enabled
        MockMasterCQL masterCQL = new MockMasterCQL();
        masterCQL.setUsesTablets(true);

        // Use two tables from our static test data
        Set<TableName> tableNames = TEST_SET_TWO_TABLES;

        // Use the statically defined table generations with two tables
        masterCQL.setTableGenerationMetadatas(TEST_TABLE_GENERATIONS);

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames)) {
            // For each table, wait for its generation to be configured
            for (TableName tableName : tableNames) {
                GenerationMetadata expectedGeneration = TEST_TABLE_GENERATIONS.get(tableName).get(0);
                // Wait for this specific table's generation to be configured using the per-table method
                masterTransportTracker.awaitConfigureWorkers(tableName, expectedGeneration);
            }
        }

        // Verify that it didn't configure any additional generations for each table
        for (TableName tableName : tableNames) {
            masterTransportTracker.checkNoAdditionalConfigureWorkers(tableName);
        }
    }

    @Test
    public void testMasterConfiguresGenerationsTabletModeWithCompletion() {
        // Test with tablet-based CDC in multi-generation mode with completion
        MockMasterTransport masterTransport = new MockMasterTransport();
        ConfigureWorkersTracker masterTransportTracker = masterTransport.tracker(DEFAULT_AWAIT);

        // Create CQL mock with tablets mode enabled
        MockMasterCQL masterCQL = new MockMasterCQL();
        masterCQL.setUsesTablets(true);

        // Use a single table with multiple generations
        Set<TableName> tableNames = TEST_SET_SINGLE_TABLE;
        TableName testTable = tableNames.iterator().next();

        // Use our data with two generations for the table
        masterCQL.setTableGenerationMetadatas(TEST_TABLE_MULTIPLE_GENERATIONS);

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames)) {
            // Get the table's generations
            List<GenerationMetadata> tableGenerations = TEST_TABLE_MULTIPLE_GENERATIONS.get(testTable);

            // Wait for the first generation to be configured
            GenerationMetadata firstGeneration = tableGenerations.get(0);
            masterTransportTracker.awaitConfigureWorkers(testTable, firstGeneration);

            awaitAreTasksFullyConsumedUntilInvocations(masterTransport);
            masterTransportTracker.checkNoAdditionalConfigureWorkers(testTable);

            masterTransport.setGenerationFullyConsumed(firstGeneration);

            // Wait for the second generation to be configured
            GenerationMetadata secondGeneration = tableGenerations.get(1);
            masterTransportTracker.awaitConfigureWorkers(testTable, secondGeneration);
        }

        // Verify that exactly two configure workers calls were made for this table
        assertEquals(2, masterTransport.getConfigureWorkersInvocationsCount(testTable));
    }

    @Test
    public void testMasterResumesFromCurrentGenerationTabletMode() {
        // Test resuming from specific generations for different tables in tablet mode

        // Create mock transport and tracker
        MockMasterTransport masterTransport = new MockMasterTransport();
        ConfigureWorkersTracker masterTransportTracker = masterTransport.tracker(DEFAULT_AWAIT);

        // Create CQL mock with tablets mode enabled
        MockMasterCQL masterCQL = new MockMasterCQL();
        masterCQL.setUsesTablets(true);

        // Use two tables, each with two generations
        Set<TableName> tableNames = Sets.newHashSet(
                new TableName("ks", "test1"),
                new TableName("ks", "test2")
        );

        // Set up test data
        masterCQL.setTableGenerationMetadatas(TEST_TABLES_MULTIPLE_GENERATIONS);

        // Configure first table to resume from its second generation
        TableName firstTable = new TableName("ks", "test1");
        TableName secondTable = new TableName("ks", "test2");

        // Get the generations for reference
        List<GenerationMetadata> firstTableGens = TEST_TABLES_MULTIPLE_GENERATIONS.get(firstTable);
        List<GenerationMetadata> secondTableGens = TEST_TABLES_MULTIPLE_GENERATIONS.get(secondTable);

        // Set current generation ID for first table to its second generation
        masterTransport.setCurrentGenerationId(firstTable,
                Optional.of(firstTableGens.get(1).getId()));

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames)) {
            // Verify that the transport received the second generation for first table
            masterTransportTracker.awaitConfigureWorkers(firstTable, firstTableGens.get(1));

            // Verify that the transport received the first generation for second table
            masterTransportTracker.awaitConfigureWorkers(secondTable, secondTableGens.get(0));

            // Mark the first generation of the second table as completed
            masterTransport.setGenerationFullyConsumed(secondTableGens.get(0));

            // Verify that the second table advances to its second generation
            masterTransportTracker.awaitConfigureWorkers(secondTable, secondTableGens.get(1));
        }

        // Verify expected number of configurations for each table
        assertEquals(1, masterTransport.getConfigureWorkersInvocationsCount(firstTable),
                "First table should have been configured once with its second generation");
        assertEquals(2, masterTransport.getConfigureWorkersInvocationsCount(secondTable),
                "Second table should have been configured twice, once for each generation2");
    }

    @Test
    public void testMasterSkipsGenerationsDueToSingleTableTabletModeTTL() {
        // Test skipping generations due to TTL in tablet mode
        MockMasterTransport masterTransport = new MockMasterTransport();
        ConfigureWorkersTracker masterTransportTracker = masterTransport.tracker(DEFAULT_AWAIT);

        // Create CQL mock with tablets mode enabled
        MockMasterCQL masterCQL = new MockMasterCQL();
        masterCQL.setUsesTablets(true);

        // Use a single table with multiple generations
        Set<TableName> tableNames = TEST_SET_SINGLE_TABLE;
        TableName testTable = tableNames.iterator().next();

        // Use table data with three generations, like in the non-tablet test:
        //  5 - 10
        // 10 - 30
        // 30 - infinity
        masterCQL.setTableGenerationMetadatas(TEST_TABLE_THREE_GENERATIONS);

        // Simulate a specific time point for TTL calculations
        Clock simulatedTime = Clock.fixed(mockTimestamp(32).toDate().toInstant(), ZoneOffset.systemDefault());

        // First test
        // We start at minute 32 with TTL of 3 minutes (provided in seconds)
        // We should skip to the second generation due to TTL
        masterCQL.setTablesTTL(Collections.singletonMap(testTable, Optional.of(3L * 60)));

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames, simulatedTime)) {
            // Get the generations for reference
            List<GenerationMetadata> generations = TEST_TABLE_THREE_GENERATIONS.get(testTable);

            // Verify the transport received the second generation (skipped the first)
            masterTransportTracker.awaitConfigureWorkers(testTable, generations.get(1));

            // Make the second generation fully consumed
            masterTransport.setGenerationFullyConsumed(generations.get(1));

            // Verify we moved to the third generation
            masterTransportTracker.awaitConfigureWorkers(testTable, generations.get(2));
        }

        // Second test
        // We start at minute 32 with TTL of 1 minute
        // We should skip directly to the last generation
        masterCQL.setTablesTTL(Collections.singletonMap(testTable, Optional.of(1L * 60)));

        // Reset transport tracking state
        masterTransport = new MockMasterTransport();
        masterTransportTracker = masterTransport.tracker(DEFAULT_AWAIT);

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames, simulatedTime)) {
            // Get the generations for reference
            List<GenerationMetadata> generations = TEST_TABLE_THREE_GENERATIONS.get(testTable);

            // Verify the transport received the third generation (skipped first and second)
            masterTransportTracker.awaitConfigureWorkers(testTable, generations.get(2));

            // Verify no further generations were processed
            masterTransportTracker.checkNoAdditionalConfigureWorkers(testTable);
        }

        // Third test
        // We start at minute 32 with TTL of 24 minutes
        // We should process all generations
        masterCQL.setTablesTTL(Collections.singletonMap(testTable, Optional.of(24L * 60)));

        // Reset transport tracking state
        masterTransport = new MockMasterTransport();
        masterTransportTracker = masterTransport.tracker(DEFAULT_AWAIT);

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames, simulatedTime)) {
            // Get the generations for reference
            List<GenerationMetadata> generations = TEST_TABLE_THREE_GENERATIONS.get(testTable);

            // Verify the first generation is configured
            masterTransportTracker.awaitConfigureWorkers(testTable, generations.get(0));

            // Mark the first generation as completed
            masterTransport.setGenerationFullyConsumed(generations.get(0));

            // Verify the second generation is configured
            masterTransportTracker.awaitConfigureWorkers(testTable, generations.get(1));

            // Mark the second generation as completed
            masterTransport.setGenerationFullyConsumed(generations.get(1));

            // Verify the third generation is configured
            masterTransportTracker.awaitConfigureWorkers(testTable, generations.get(2));
        }

        // Fourth test
        // We start at minute 32 with no TTL
        // We should process all generations
        masterCQL.setTablesTTL(Collections.singletonMap(testTable, Optional.empty()));

        // Reset transport tracking state
        masterTransport = new MockMasterTransport();
        masterTransportTracker = masterTransport.tracker(DEFAULT_AWAIT);

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames, simulatedTime)) {
            // Get the generations for reference
            List<GenerationMetadata> generations = TEST_TABLE_THREE_GENERATIONS.get(testTable);

            // Verify the first generation is configured
            masterTransportTracker.awaitConfigureWorkers(testTable, generations.get(0));

            // Mark the first generation as completed
            masterTransport.setGenerationFullyConsumed(generations.get(0));

            // Verify the second generation is configured
            masterTransportTracker.awaitConfigureWorkers(testTable, generations.get(1));

            // Mark the second generation as completed
            masterTransport.setGenerationFullyConsumed(generations.get(1));

            // Verify the third generation is configured
            masterTransportTracker.awaitConfigureWorkers(testTable, generations.get(2));
        }
    }

    @Test
    public void testMasterResilientToCQLExceptionsTabletMode() {
        // Test that Master is resilient to CQL exceptions in tablet mode
        MockMasterTransport masterTransport = new MockMasterTransport();
        ConfigureWorkersTracker masterTransportTracker = masterTransport.tracker(DEFAULT_AWAIT);

        // Create CQL mock with tablets mode enabled
        MockMasterCQL masterCQL = new MockMasterCQL();
        masterCQL.setUsesTablets(true);

        // Use a single table with multiple generations
        Set<TableName> tableNames = TEST_SET_SINGLE_TABLE;
        TableName testTable = tableNames.iterator().next();

        // Use table data with two generations
        masterCQL.setTableGenerationMetadatas(TEST_TABLE_MULTIPLE_GENERATIONS);

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames)) {
            // Get the generations for reference
            List<GenerationMetadata> generations = TEST_TABLE_MULTIPLE_GENERATIONS.get(testTable);

            // Verify that the transport received the first generation
            masterTransportTracker.awaitConfigureWorkers(testTable, generations.get(0));

            // We simulate constant CQL failure...
            masterCQL.setShouldInjectFailure(true);

            // ...mark the first generation as completed to trigger transition...
            masterTransport.setGenerationFullyConsumed(generations.get(0));

            // ...CQL queries are performed, but they fail and no new generation is discovered...
            awaitCQLInvocations(masterCQL);
            masterTransportTracker.checkNoAdditionalConfigureWorkers(testTable);

            // ...the CQL is back working...
            masterCQL.setShouldInjectFailure(false);

            // ...and observe moving to the next generation.
            masterTransportTracker.awaitConfigureWorkers(testTable, generations.get(1));
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
