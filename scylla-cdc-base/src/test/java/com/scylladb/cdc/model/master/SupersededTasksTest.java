package com.scylladb.cdc.model.master;

import com.google.common.collect.Lists;
import com.scylladb.cdc.cql.MockMasterCQL;
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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.scylladb.cdc.model.master.MockGenerationMetadata.generationMetadataToTaskMap;
import static com.scylladb.cdc.model.master.MockGenerationMetadata.mockGenerationMetadata;
import static com.scylladb.cdc.model.master.MockGenerationMetadata.tabletGenerationMetadataToTaskMap;
import static org.awaitility.Awaitility.with;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The tasks the master passes as superseded: those of the stored generation, and only when the
 * first configure moves past it.
 */
public class SupersededTasksTest {
    private static final ConditionFactory AWAIT =
            with().pollInterval(1, TimeUnit.MILLISECONDS).await().atMost(2000, TimeUnit.MILLISECONDS);
    private static final TableName TABLE = new TableName("ks", "t");
    private static final Set<TableName> TABLES = Collections.singleton(TABLE);

    // Minutes after epoch: 5-10, 10-15, 15-infinity.
    private static final GenerationMetadata G1 = mockGenerationMetadata(minute(5), Optional.of(minute(10)), 2, 2);
    private static final GenerationMetadata G2 = mockGenerationMetadata(minute(10), Optional.of(minute(15)), 2, 2);
    private static final GenerationMetadata G3 = mockGenerationMetadata(minute(15), Optional.empty(), 2, 2);
    private static final List<GenerationMetadata> GENERATIONS = Lists.newArrayList(G1, G2, G3);

    // Past G1's end, before G2's: with a one-minute TTL only G1 has expired.
    private static final Clock MINUTE_14 = Clock.fixed(minute(14).toDate().toInstant(), ZoneOffset.UTC);

    private final MockMasterTransport transport = new MockMasterTransport();
    private final MockMasterCQL cql = new MockMasterCQL(GENERATIONS);

    SupersededTasksTest() {
        cql.setTableGenerationMetadatas(Collections.singletonMap(TABLE, GENERATIONS));
    }

    @Test
    public void tabletStoredGenerationConsumedIsSuperseded() throws Exception {
        transport.setCurrentGenerationId(TABLE, Optional.of(G1.getId()));
        transport.setGenerationFullyConsumed(G2);

        tabletController(Clock.systemUTC()).initCurrentGeneration();

        assertEquals(tabletGenerationMetadataToTaskMap(G3, TABLE), transport.getConfigureWorkersInvocation(TABLE, 0));
        assertEquals(tabletTasksWithLegacy(G1), transport.getSupersededTasks(TABLE, 0));
    }

    @Test
    public void tabletStoredGenerationPastTtlIsSuperseded() throws Exception {
        transport.setCurrentGenerationId(TABLE, Optional.of(G1.getId()));
        cql.setTablesTTL(Collections.singletonMap(TABLE, Optional.of(60L)));

        tabletController(MINUTE_14).initCurrentGeneration();

        assertEquals(tabletGenerationMetadataToTaskMap(G2, TABLE), transport.getConfigureWorkersInvocation(TABLE, 0));
        assertEquals(tabletTasksWithLegacy(G1), transport.getSupersededTasks(TABLE, 0));
    }

    @Test
    public void tabletResumeSupersedesNothing() throws Exception {
        transport.setCurrentGenerationId(TABLE, Optional.of(G2.getId()));

        tabletController(Clock.systemUTC()).initCurrentGeneration();

        assertEquals(tabletGenerationMetadataToTaskMap(G2, TABLE), transport.getConfigureWorkersInvocation(TABLE, 0));
        assertEquals(Collections.emptySet(), transport.getSupersededTasks(TABLE, 0));
    }

    @Test
    public void tabletFirstStartSupersedesNothing() throws Exception {
        transport.setGenerationFullyConsumed(G1);

        tabletController(Clock.systemUTC()).initCurrentGeneration();

        assertEquals(tabletGenerationMetadataToTaskMap(G2, TABLE), transport.getConfigureWorkersInvocation(TABLE, 0));
        assertEquals(Collections.emptySet(), transport.getSupersededTasks(TABLE, 0));
    }

    @Test
    public void tabletSwitchAfterStartSupersedesNothing() throws Exception {
        transport.setCurrentGenerationId(TABLE, Optional.of(G1.getId()));
        TableCDCController controller = tabletController(Clock.systemUTC());
        controller.initCurrentGeneration();
        transport.setGenerationFullyConsumed(G1);

        controller.runMasterStep();

        assertEquals(tabletGenerationMetadataToTaskMap(G2, TABLE), transport.getConfigureWorkersInvocation(TABLE, 1));
        assertEquals(Collections.emptySet(), transport.getSupersededTasks(TABLE, 0));
        assertEquals(Collections.emptySet(), transport.getSupersededTasks(TABLE, 1));
    }

    @Test
    public void vnodeStoredGenerationConsumedIsSuperseded() {
        transport.setCurrentGenerationId(Optional.of(G1.getId()));
        transport.setGenerationFullyConsumed(G2);

        try (MasterThread master = new MasterThread(transport, cql, TABLES)) {
            tracker().awaitConfigureWorkers(G3, TABLES);
        }

        assertEquals(generationMetadataToTaskMap(G1, TABLES).keySet(), transport.getSupersededTasks(0));
    }

    @Test
    public void vnodeStoredGenerationPastTtlIsSuperseded() {
        transport.setCurrentGenerationId(Optional.of(G1.getId()));
        cql.setTablesTTL(Collections.singletonMap(TABLE, Optional.of(60L)));

        ConfigureWorkersTracker tracker = tracker();

        try (MasterThread master = new MasterThread(transport, cql, TABLES, MINUTE_14)) {
            tracker.awaitConfigureWorkers(G2, TABLES);
            transport.setGenerationFullyConsumed(G2);
            tracker.awaitConfigureWorkers(G3, TABLES);
        }

        assertEquals(generationMetadataToTaskMap(G1, TABLES).keySet(), transport.getSupersededTasks(0));
        // Later switches are diffed by the transport.
        assertEquals(Collections.emptySet(), transport.getSupersededTasks(1));
    }

    @Test
    public void vnodeResumeSupersedesNothing() {
        transport.setCurrentGenerationId(Optional.of(G2.getId()));

        try (MasterThread master = new MasterThread(transport, cql, TABLES)) {
            tracker().awaitConfigureWorkers(G2, TABLES);
        }

        assertEquals(Collections.emptySet(), transport.getSupersededTasks(0));
    }

    @Test
    public void vnodeFirstStartAndLaterSwitchSupersedeNothing() {
        transport.setGenerationFullyConsumed(G1);
        ConfigureWorkersTracker tracker = tracker();

        try (MasterThread master = new MasterThread(transport, cql, TABLES)) {
            tracker.awaitConfigureWorkers(G2, TABLES);
            transport.setGenerationFullyConsumed(G2);
            tracker.awaitConfigureWorkers(G3, TABLES);
        }

        assertEquals(Collections.emptySet(), transport.getSupersededTasks(0));
        assertEquals(Collections.emptySet(), transport.getSupersededTasks(1));
    }

    private TableCDCController tabletController(Clock clock) {
        return new TableCDCController(TABLE, MasterConfiguration.builder()
                .withTransport(transport).withCQL(cql).addTable(TABLE).withClock(clock).build());
    }

    private ConfigureWorkersTracker tracker() {
        return transport.tracker(AWAIT);
    }

    private static Set<TaskId> tabletTasksWithLegacy(GenerationMetadata generation) {
        Set<TaskId> tasks = new HashSet<>(tabletGenerationMetadataToTaskMap(generation, TABLE).keySet());
        tasks.add(TaskId.legacyTabletTask(generation.getId(), TABLE));
        return tasks;
    }

    private static Timestamp minute(long minutesAfterEpoch) {
        return new Timestamp(new Date(TimeUnit.MINUTES.toMillis(minutesAfterEpoch)));
    }
}
