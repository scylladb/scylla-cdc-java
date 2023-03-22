package com.scylladb.cdc.model.worker;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.master.GenerationMetadata;
import com.scylladb.cdc.model.master.MockGenerationMetadata;
import com.scylladb.cdc.transport.WorkerTransport;

import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.fail;

public class WorkerThread implements AutoCloseable {
    public static final long DEFAULT_QUERY_WINDOW_SIZE_MS = 5;
    public static final long DEFAULT_CONFIDENCE_WINDOW_SIZE_MS = 10;

    private static final long FUTURE_GET_TIMEOUT = 3000;
    private final Worker worker;
    private final Future<Throwable> workerRunFuture;

    public WorkerThread(WorkerConfiguration workerConfiguration, Map<TaskId, SortedSet<StreamId>> groupedStreams) {
        Preconditions.checkNotNull(workerConfiguration);
        this.worker = new Worker(workerConfiguration);
        this.workerRunFuture = Executors.newSingleThreadExecutor().submit(() -> {
            try {
                worker.run(groupedStreams);
                return null;
            } catch (Throwable t) {
                return t;
            }
        });
    }

    public WorkerThread(WorkerCQL workerCQL, WorkerTransport workerTransport, Consumer consumer, Clock clock,
                        Map<TaskId, SortedSet<StreamId>> groupedStreams) {
        this(WorkerConfiguration.builder()
                .withCQL(workerCQL)
                .withTransport(workerTransport)
                .withConsumer(consumer)
                .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
                .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
                .withClock(clock)
                .withMinimalWaitForWindowMs(WorkerConfiguration.DEFAULT_MINIMAL_WAIT_FOR_WINDOW_MS)
                .build(), groupedStreams);
    }

    public WorkerThread(WorkerCQL workerCQL, WorkerTransport workerTransport, Consumer consumer,
                        GenerationMetadata generationMetadata, Clock clock, Set<TableName> tableNames) {
        this(workerCQL, workerTransport, consumer, clock, MockGenerationMetadata.generationMetadataToTaskMap(generationMetadata, tableNames));
    }

    public WorkerThread(WorkerCQL workerCQL, WorkerTransport workerTransport, Consumer consumer,
                        GenerationMetadata generationMetadata, TableName tableName) {
        this(workerCQL, workerTransport, consumer, generationMetadata, Clock.systemDefaultZone(), tableName);
    }

    public WorkerThread(WorkerCQL workerCQL, WorkerTransport workerTransport, Consumer consumer,
                        GenerationMetadata generationMetadata, Clock clock, TableName tableName) {
        this(workerCQL, workerTransport, consumer, generationMetadata, clock, Collections.singleton(tableName));
    }

    @Override
    public void close() {
        if (this.worker != null) {
            this.worker.stop();
        }
        if (this.workerRunFuture != null) {
            try {
                Throwable t = this.workerRunFuture.get(FUTURE_GET_TIMEOUT, TimeUnit.MILLISECONDS);
                if (t != null) {
                    fail("Worker run future threw exception", t);
                }
            } catch (Exception e) {
                fail("Could not successfully get() worker future", e);
            }
        }
    }
}
