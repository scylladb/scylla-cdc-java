package com.scylladb.cdc.lib;

import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.master.GenerationBasedCDCMetadataModel;
import com.scylladb.cdc.model.master.GenerationMetadata;
import com.scylladb.cdc.model.master.MasterConfiguration;
import com.scylladb.cdc.model.master.TableCDCController;
import com.scylladb.cdc.model.worker.Task;
import com.scylladb.cdc.model.worker.TaskState;
import com.scylladb.cdc.model.worker.WorkerConfiguration;
import com.scylladb.cdc.transport.GroupedTasks;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Generation switches in {@link LocalTransport} with a durable store: when old checkpoints are
 * deleted relative to saving the new generation, and what a failure in between leaves behind.
 */
class LocalTransportGenerationSwitchTest {
    private static final TableName TABLE = new TableName("ks", "t");
    private static final long T0 = 1_700_000_000_000L;
    private static final long T1 = T0 + 60_000L;
    private static final GenerationMetadata G1_OPEN = generation(T0, null);
    private static final GenerationMetadata G1 = generation(T0, T1);
    private static final GenerationMetadata G2 = generation(T1, null);

    private final FlakyStore store = new FlakyStore();
    private final FakeMasterCQL cql = new FakeMasterCQL();
    private final PendingWorkerCQL workerCql = new PendingWorkerCQL();
    private final List<ScheduledThreadPoolExecutor> executors = new ArrayList<>();
    private final List<LocalTransport> transports = new ArrayList<>();

    @AfterEach
    void stopTransports() throws Exception {
        for (LocalTransport transport : transports) {
            transport.stop();
        }
    }

    @Test
    void vnodeSwitchSavesGenerationAndStopsWorkerBeforeDeletingOldCheckpoints() throws Exception {
        LocalTransport transport = newTransport();
        transport.configureWorkers(vnodeTasks(G1));
        assertEquals(vnodeTaskIds(G1), storedOf(vnodeTaskIds(G1)));
        store.ops.clear();
        store.onDelete = () -> store.ops.add("old worker stopped: " + executors.get(0).isTerminated());

        transport.configureWorkers(vnodeTasks(G2));

        assertEquals(List.of("save " + G2.getId(), "old worker stopped: true",
                "delete " + new TreeSet<>(vnodeTaskIds(G1))), store.ops.subList(0, 3));
        assertTrue(storedOf(vnodeTaskIds(G1)).isEmpty());
    }

    @Test
    void tabletSwitchSavesGenerationBeforeDeletingOldCheckpoints() throws Exception {
        LocalTransport transport = newTransport();
        transport.configureWorkers(TABLE, tabletTasks(G1));
        store.ops.clear();

        transport.configureWorkers(TABLE, tabletTasks(G2));

        assertEquals(List.of("save " + TABLE + " " + G2.getId(), "delete " + new TreeSet<>(tabletTaskIds(G1))),
                store.ops.subList(0, 2));
        assertTrue(storedOf(tabletTaskIds(G1)).isEmpty());
    }

    @Test
    void vnodeFailedSaveKeepsOldCheckpointsAndGeneration() throws Exception {
        LocalTransport transport = newTransport();
        transport.configureWorkers(vnodeTasks(G1));
        store.failGenerationSave = true;

        assertThrows(IllegalStateException.class, () -> transport.configureWorkers(vnodeTasks(G2)));

        assertEquals(vnodeTaskIds(G1), storedOf(vnodeTaskIds(G1)));
        assertEquals(Optional.of(G1.getId()), transport.getCurrentGenerationId());
    }

    @Test
    void tabletFailedSaveKeepsOldCheckpointsAndGeneration() throws Exception {
        LocalTransport transport = newTransport();
        transport.configureWorkers(TABLE, tabletTasks(G1));
        store.failGenerationSave = true;

        assertThrows(IllegalStateException.class, () -> transport.configureWorkers(TABLE, tabletTasks(G2)));

        assertEquals(tabletTaskIds(G1), storedOf(tabletTaskIds(G1)));
        assertEquals(Optional.of(G1.getId()), transport.getCurrentGenerationId(TABLE));
    }

    @Test
    void vnodeFailedDeleteStillStartsTheNewGeneration() throws Exception {
        LocalTransport transport = newTransport();
        transport.configureWorkers(vnodeTasks(G1));
        store.failDelete = true;

        transport.configureWorkers(vnodeTasks(G2));

        assertEquals(Optional.of(G2.getId()), store.loadGenerationId());
        assertEquals(vnodeTaskIds(G2), storedOf(vnodeTaskIds(G2)));
    }

    @Test
    void tabletFailedDeleteStillStartsTheNewGeneration() throws Exception {
        LocalTransport transport = newTransport();
        transport.configureWorkers(TABLE, tabletTasks(G1));
        store.failDelete = true;

        transport.configureWorkers(TABLE, tabletTasks(G2));

        assertEquals(Optional.of(G2.getId()), store.loadGenerationId(TABLE));
        assertEquals(tabletTaskIds(G2), storedOf(tabletTaskIds(G2)));
    }

    @Test
    void tabletMasterRetryAfterFailedSaveDoesNotRereadOldGeneration() throws Exception {
        cql.tableGenerations = List.of(G1_OPEN);
        LocalTransport transport = newTransport();
        TableCDCController first = new TableCDCController(TABLE, masterConfiguration(transport));
        first.initCurrentGeneration();
        consumedToEnd(tabletTaskIds(G1));
        cql.tableGenerations = List.of(G1, G2);
        store.failGenerationSave = true;
        assertThrows(IllegalStateException.class, first::runMasterStep);
        store.failGenerationSave = false;

        // Master.run retries in-process: same transport, new model, workers stopped first.
        transport.stopWorkers();
        new TableCDCController(TABLE, masterConfiguration(transport)).initCurrentGeneration();

        assertTrue(storedOf(tabletTaskIds(G1)).isEmpty());
        assertEquals(Optional.of(G2.getId()), store.loadGenerationId(TABLE));
        assertEquals(tabletTaskIds(G2), storedOf(tabletTaskIds(G2)));
    }

    @Test
    void vnodeMasterRetryAfterFailedSaveDoesNotRereadOldGeneration() throws Exception {
        cql.generations = List.of(G1_OPEN);
        LocalTransport transport = newTransport();
        Thread first = runVnodeMaster(transport);
        await(() -> storedOf(vnodeTaskIds(G1)).equals(vnodeTaskIds(G1)));
        consumedToEnd(vnodeTaskIds(G1));
        store.failGenerationSave = true;
        cql.generations = List.of(G1, G2);
        first.join(5_000);
        assertFalse(first.isAlive(), "the failed save ends the master loop");
        store.failGenerationSave = false;

        // Master.run retries in-process: same transport, new model.
        Thread retry = runVnodeMaster(transport);
        try {
            await(() -> store.loadGenerationId().equals(Optional.of(G2.getId()))
                    && storedOf(vnodeTaskIds(G1)).isEmpty()
                    && storedOf(vnodeTaskIds(G2)).equals(vnodeTaskIds(G2)));
        } finally {
            retry.interrupt();
            retry.join(5_000);
        }
    }

    @Test
    void tabletRestartPastDoneGenerationDeletesItsCheckpoints() throws Exception {
        consumeTabletG1ThenRestart();
        // A checkpoint from before tablet streams got one task each, left behind by a failed delete.
        TaskId legacy = TaskId.legacyTabletTask(G1.getId(), TABLE);
        consumedToEnd(Collections.singleton(legacy));
        LocalTransport transport = newTransport();

        new TableCDCController(TABLE, masterConfiguration(transport)).initCurrentGeneration();

        assertTrue(storedOf(tabletTaskIds(G1)).isEmpty());
        assertTrue(storedOf(Collections.singleton(legacy)).isEmpty());
        assertEquals(Optional.of(G2.getId()), store.loadGenerationId(TABLE));
        assertEquals(tabletTaskIds(G2), storedOf(tabletTaskIds(G2)));
    }

    @Test
    void vnodeRestartPastDoneGenerationDeletesItsCheckpoints() throws Exception {
        cql.generations = List.of(G1_OPEN);
        LocalTransport before = newTransport();
        Thread first = runVnodeMaster(before);
        await(() -> storedOf(vnodeTaskIds(G1)).equals(vnodeTaskIds(G1)));
        first.interrupt();
        first.join(5_000);
        before.stop();
        consumedToEnd(vnodeTaskIds(G1));
        cql.generations = List.of(G1, G2);

        Thread restarted = runVnodeMaster(newTransport());
        try {
            await(() -> store.loadGenerationId().equals(Optional.of(G2.getId()))
                    && storedOf(vnodeTaskIds(G1)).isEmpty()
                    && storedOf(vnodeTaskIds(G2)).equals(vnodeTaskIds(G2)));
        } finally {
            restarted.interrupt();
            restarted.join(5_000);
        }
    }

    @Test
    void restartDeletesSkippedGenerationEvenIfWorkerStartFails() throws Exception {
        consumeTabletG1ThenRestart();
        workerCql.prepareFailure = new ExecutionException(new IllegalStateException("injected"));
        LocalTransport transport = newTransport();

        assertThrows(RuntimeException.class,
                () -> new TableCDCController(TABLE, masterConfiguration(transport)).initCurrentGeneration());

        assertTrue(storedOf(tabletTaskIds(G1)).isEmpty());
        assertEquals(Optional.of(G2.getId()), store.loadGenerationId(TABLE));
    }

    @Test
    void restartFailedDeleteStillStartsTheNewGeneration() throws Exception {
        consumeTabletG1ThenRestart();
        store.failDelete = true;
        LocalTransport transport = newTransport();

        new TableCDCController(TABLE, masterConfiguration(transport)).initCurrentGeneration();

        assertEquals(Optional.of(G2.getId()), store.loadGenerationId(TABLE));
        assertEquals(tabletTaskIds(G2), storedOf(tabletTaskIds(G2)));
    }

    @Test
    void restartRetryAfterFailedSaveDeletesSkippedGeneration() throws Exception {
        consumeTabletG1ThenRestart();
        LocalTransport transport = newTransport();
        store.failGenerationSave = true;
        assertThrows(IllegalStateException.class,
                () -> new TableCDCController(TABLE, masterConfiguration(transport)).initCurrentGeneration());
        assertEquals(tabletTaskIds(G1), storedOf(tabletTaskIds(G1)));
        store.failGenerationSave = false;

        // Master.run retries in-process: same transport, new model, workers stopped first.
        transport.stopWorkers();
        new TableCDCController(TABLE, masterConfiguration(transport)).initCurrentGeneration();

        assertTrue(storedOf(tabletTaskIds(G1)).isEmpty());
        assertEquals(Optional.of(G2.getId()), store.loadGenerationId(TABLE));
    }

    // Consumes G1 to its end with one transport, stops it, and closes G1; the next transport is the restart.
    private void consumeTabletG1ThenRestart() throws Exception {
        cql.tableGenerations = List.of(G1_OPEN);
        LocalTransport before = newTransport();
        new TableCDCController(TABLE, masterConfiguration(before)).initCurrentGeneration();
        before.stop();
        consumedToEnd(tabletTaskIds(G1));
        cql.tableGenerations = List.of(G1, G2);
    }

    private LocalTransport newTransport() {
        Supplier<ScheduledExecutorService> executorSupplier = () -> {
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            executors.add(executor);
            return executor;
        };
        LocalTransport transport = new LocalTransport(WorkerConfiguration.builder()
                .withCQL(workerCql)
                .withConsumer(change -> CompletableFuture.completedFuture(null)),
                executorSupplier, new StoreBackedTaskStateBackend(store));
        transports.add(transport);
        return transport;
    }

    private MasterConfiguration masterConfiguration(LocalTransport transport) {
        return MasterConfiguration.builder().withTransport(transport).withCQL(cql).addTable(TABLE)
                .withSleepBeforeGenerationDoneMs(10).build();
    }

    private Thread runVnodeMaster(LocalTransport transport) {
        GenerationBasedCDCMetadataModel model = new GenerationBasedCDCMetadataModel(masterConfiguration(transport));
        Thread thread = new Thread(() -> {
            try {
                model.runMasterLoop();
            } catch (Exception expected) {
                // A failed configureWorkers ends the loop, as it does under Master.run.
            }
        });
        thread.start();
        return thread;
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        for (int i = 0; i < 500 && !condition.getAsBoolean(); i++) {
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean());
    }

    private Set<TaskId> storedOf(Set<TaskId> tasks) {
        return store.loadTaskStates(tasks).keySet();
    }

    // Written straight to the store: the fake readers never return, so the worker writes nothing else.
    private void consumedToEnd(Set<TaskId> tasks) {
        Timestamp past = new Timestamp(new Date(T1 + 1_000L));
        tasks.forEach(task -> store.saveTaskState(task,
                new TaskState(past, new Timestamp(new Date(T1 + 2_000L)), Optional.empty())));
    }

    private static GenerationMetadata generation(long start, Long end) {
        SortedSet<StreamId> streams = new TreeSet<>(List.of(stream(start, 1), stream(start, 2)));
        return new GenerationMetadata(new Timestamp(new Date(start)),
                Optional.ofNullable(end).map(e -> new Timestamp(new Date(e))), streams);
    }

    // Version 1 stream IDs with vnode index 0: one vnode task per table, one tablet task per stream.
    private static StreamId stream(long generationStart, long token) {
        ByteBuffer value = ByteBuffer.allocate(16);
        value.putLong(generationStart + token);
        value.putLong(1L);
        value.flip();
        return new StreamId(value);
    }

    private static GroupedTasks vnodeTasks(GenerationMetadata generation) {
        Map<TaskId, SortedSet<StreamId>> tasks = new HashMap<>();
        for (StreamId stream : generation.getStreams()) {
            tasks.computeIfAbsent(new TaskId(generation.getId(), stream.getVNodeId(), TABLE),
                    id -> new TreeSet<>()).add(stream);
        }
        return new GroupedTasks(tasks, generation);
    }

    private static GroupedTasks tabletTasks(GenerationMetadata generation) {
        Map<TaskId, SortedSet<StreamId>> tasks = new HashMap<>();
        int index = 0;
        for (StreamId stream : generation.getStreams()) {
            tasks.put(TaskId.forTabletStream(generation.getId(), index++, TABLE),
                    new TreeSet<>(Collections.singleton(stream)));
        }
        return new GroupedTasks(tasks, generation);
    }

    private static Set<TaskId> vnodeTaskIds(GenerationMetadata generation) {
        return vnodeTasks(generation).getTaskIds();
    }

    private static Set<TaskId> tabletTaskIds(GenerationMetadata generation) {
        return tabletTasks(generation).getTaskIds();
    }

    /** Records generation saves and deletes in order, and fails them on demand. */
    static final class FlakyStore extends InMemoryStateStore {
        final List<String> ops = Collections.synchronizedList(new ArrayList<>());
        volatile boolean failGenerationSave;
        volatile boolean failDelete;
        volatile Runnable onDelete = () -> { };

        @Override
        public void saveGenerationId(GenerationId generationId) {
            if (failGenerationSave) {
                throw new IllegalStateException("injected generation save failure");
            }
            ops.add("save " + generationId);
            super.saveGenerationId(generationId);
        }

        @Override
        public void saveGenerationId(TableName table, GenerationId generationId) {
            if (failGenerationSave) {
                throw new IllegalStateException("injected generation save failure");
            }
            ops.add("save " + table + " " + generationId);
            super.saveGenerationId(table, generationId);
        }

        @Override
        public void deleteTaskStates(Set<TaskId> tasks) {
            onDelete.run();
            if (failDelete) {
                throw new IllegalStateException("injected checkpoint delete failure");
            }
            ops.add("delete " + new TreeSet<>(tasks));
            super.deleteTaskStates(tasks);
        }
    }

    /** Serves fixed generation lists; tests swap the lists to close a generation. */
    static final class FakeMasterCQL implements MasterCQL {
        volatile List<GenerationMetadata> generations = List.of();
        volatile List<GenerationMetadata> tableGenerations = List.of();

        @Override
        public CompletableFuture<Optional<Long>> fetchTableTTL(TableName tableName) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletableFuture<Optional<Throwable>> validateTable(TableName table) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public Boolean usesTablets(TableName tableName) {
            return !tableGenerations.isEmpty();
        }

        @Override
        public CompletableFuture<Optional<GenerationId>> fetchFirstGenerationId() {
            return CompletableFuture.completedFuture(generations.stream().findFirst().map(GenerationMetadata::getId));
        }

        @Override
        public CompletableFuture<GenerationMetadata> fetchGenerationMetadata(GenerationId id) {
            return CompletableFuture.completedFuture(find(generations, id));
        }

        @Override
        public CompletableFuture<Optional<Timestamp>> fetchGenerationEnd(GenerationId id) {
            return CompletableFuture.completedFuture(find(generations, id).getEnd());
        }

        @Override
        public CompletableFuture<GenerationId> fetchFirstTableGenerationId(TableName table) {
            return CompletableFuture.completedFuture(tableGenerations.get(0).getId());
        }

        @Override
        public CompletableFuture<GenerationMetadata> fetchTableGenerationMetadata(TableName table, GenerationId id) {
            return CompletableFuture.completedFuture(find(tableGenerations, id));
        }

        @Override
        public CompletableFuture<Optional<Timestamp>> fetchTableGenerationEnd(TableName table, GenerationId id) {
            return CompletableFuture.completedFuture(find(tableGenerations, id).getEnd());
        }

        private static GenerationMetadata find(List<GenerationMetadata> list, GenerationId id) {
            return list.stream().filter(g -> g.getId().equals(id)).findFirst()
                    .orElseThrow(() -> new IllegalStateException("No generation " + id));
        }
    }

    private static final class PendingWorkerCQL implements WorkerCQL {
        volatile ExecutionException prepareFailure;

        @Override
        public void prepare(Set<TableName> tables) throws ExecutionException {
            if (prepareFailure != null) {
                throw prepareFailure;
            }
        }

        @Override
        public CompletableFuture<Reader> createReader(Task task) {
            return new CompletableFuture<>();
        }

        @Override
        public CompletableFuture<Optional<Long>> fetchTableTTL(TableName tableName) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
    }
}
