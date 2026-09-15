package com.scylladb.cdc.lib;

import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.VNodeId;
import com.scylladb.cdc.model.master.GenerationMetadata;
import com.scylladb.cdc.model.worker.Task;
import com.scylladb.cdc.model.worker.WorkerConfiguration;
import com.scylladb.cdc.transport.GroupedTasks;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
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
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalTransportLifecycleTest {
    private static final GenerationId GENERATION =
            new GenerationId(new Timestamp(new Date(1_700_000_000_000L)));
    private static final TableName FIRST_TABLE = new TableName("ks", "first_table");
    private static final TableName SECOND_TABLE = new TableName("ks", "second_table");

    @Test
    void uncheckedStartupFailureShutsDownExecutorAndAllowsRetry() throws Exception {
        RecordingExecutorSupplier executors = new RecordingExecutorSupplier();
        LocalTransport transport = transport(new PendingWorkerCQL(), executors);

        assertThrows(IllegalArgumentException.class,
                () -> transport.configureWorkers(malformedTabletTasks(FIRST_TABLE)));

        assertTrue(transport.isReadyToStart());
        assertEquals(1, executors.created.size());
        assertTrue(executors.created.get(0).isTerminated());

        transport.configureWorkers(vnodeTasks(FIRST_TABLE));
        assertFalse(transport.isReadyToStart());
        transport.stop();

        assertTrue(transport.isReadyToStart());
        assertTrue(executors.created.get(1).isTerminated());
    }

    @Test
    void checkedStartupFailureShutsDownExecutorAndAllowsRetry() throws Exception {
        RecordingExecutorSupplier executors = new RecordingExecutorSupplier();
        PendingWorkerCQL cql = new PendingWorkerCQL();
        cql.prepareFailure = new ExecutionException(new IllegalStateException("injected"));
        LocalTransport transport = transport(cql, executors);

        RuntimeException failure = assertThrows(RuntimeException.class,
                () -> transport.configureWorkers(vnodeTasks(FIRST_TABLE)));

        assertInstanceOf(ExecutionException.class, failure.getCause());
        assertTrue(transport.isReadyToStart());
        assertTrue(executors.created.get(0).isTerminated());

        cql.prepareFailure = null;
        transport.configureWorkers(vnodeTasks(FIRST_TABLE));
        assertFalse(transport.isReadyToStart());
        transport.stop();

        assertTrue(executors.created.get(1).isTerminated());
    }

    @Test
    void failedSecondTabletTableStopsSharedWorkerAndFullRetrySucceeds() throws Exception {
        RecordingExecutorSupplier executors = new RecordingExecutorSupplier();
        LocalTransport transport = transport(new PendingWorkerCQL(), executors);

        transport.configureWorkers(FIRST_TABLE, tabletTasks(FIRST_TABLE));
        assertThrows(IllegalArgumentException.class,
                () -> transport.configureWorkers(
                        SECOND_TABLE, malformedTabletTasks(SECOND_TABLE)));

        assertTrue(transport.isReadyToStart());
        assertTrue(executors.created.get(0).isTerminated());

        transport.configureWorkers(FIRST_TABLE, tabletTasks(FIRST_TABLE));
        transport.configureWorkers(SECOND_TABLE, tabletTasks(SECOND_TABLE));
        assertFalse(transport.isReadyToStart());
        transport.stop();

        assertTrue(transport.isReadyToStart());
        assertTrue(executors.created.get(1).isTerminated());
    }

    @Test
    void emptyAssignmentDoesNotCreateWorkerOrExecutor() throws Exception {
        RecordingExecutorSupplier executors = new RecordingExecutorSupplier();
        LocalTransport transport = transport(new PendingWorkerCQL(), executors);
        GenerationMetadata metadata = metadata(Collections.emptySortedSet());

        transport.configureWorkers(new GroupedTasks(Collections.emptyMap(), metadata));

        assertTrue(transport.isReadyToStart());
        assertTrue(executors.created.isEmpty());
    }

    private static LocalTransport transport(WorkerCQL cql,
                                            Supplier<ScheduledExecutorService> executors) {
        WorkerConfiguration.Builder configuration = WorkerConfiguration.builder()
                .withCQL(cql)
                .withConsumer(change -> CompletableFuture.completedFuture(null));
        return new LocalTransport(configuration, executors);
    }

    private static GroupedTasks vnodeTasks(TableName table) {
        StreamId stream = stream(1);
        SortedSet<StreamId> streams = singletonStream(stream);
        TaskId taskId = new TaskId(GENERATION, new VNodeId(0), table);
        return new GroupedTasks(Collections.singletonMap(taskId, streams), metadata(streams));
    }

    private static GroupedTasks tabletTasks(TableName table) {
        StreamId stream = stream(1);
        SortedSet<StreamId> streams = singletonStream(stream);
        TaskId taskId = TaskId.forTabletStream(GENERATION, 0, table);
        return new GroupedTasks(Collections.singletonMap(taskId, streams), metadata(streams));
    }

    private static GroupedTasks malformedTabletTasks(TableName table) {
        StreamId first = stream(1);
        StreamId second = stream(2);
        SortedSet<StreamId> streams = new TreeSet<>(Set.of(first, second));
        Map<TaskId, SortedSet<StreamId>> tasks = Map.of(
                new TaskId(GENERATION, new VNodeId(0), table), singletonStream(first),
                TaskId.forTabletStream(GENERATION, 0, table), singletonStream(second));
        return new GroupedTasks(tasks, metadata(streams));
    }

    private static GenerationMetadata metadata(SortedSet<StreamId> streams) {
        return new GenerationMetadata(GENERATION.getGenerationStart(), Optional.empty(), streams);
    }

    private static SortedSet<StreamId> singletonStream(StreamId stream) {
        return new TreeSet<>(Collections.singleton(stream));
    }

    private static StreamId stream(long token) {
        ByteBuffer value = ByteBuffer.allocate(16);
        value.putLong(token);
        value.putLong(1L);
        value.flip();
        return new StreamId(value);
    }

    private static final class RecordingExecutorSupplier
            implements Supplier<ScheduledExecutorService> {
        private final List<ScheduledThreadPoolExecutor> created = new ArrayList<>();

        @Override
        public ScheduledExecutorService get() {
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            created.add(executor);
            return executor;
        }
    }

    private static final class PendingWorkerCQL implements WorkerCQL {
        private ExecutionException prepareFailure;

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
