package com.scylladb.cdc.model.master;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.transport.MasterTransport;

public final class Master {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private static final long SLEEP_BEFORE_FIRST_GENERATION_MS = TimeUnit.SECONDS.toMillis(10);
    private static final long SLEEP_BEFORE_GENERATION_DONE_MS = TimeUnit.SECONDS.toMillis(30);
    private static final long SLEEP_AFTER_EXCEPTION_MS = TimeUnit.SECONDS.toMillis(10);

    private final MasterTransport transport;
    private final MasterCQL cql;
    private final Set<TableName> tables;

    public Master(MasterTransport transport, MasterCQL cql, Set<TableName> tables) {
        this.transport = Preconditions.checkNotNull(transport);
        this.cql = Preconditions.checkNotNull(cql);
        Preconditions.checkNotNull(tables);
        Preconditions.checkArgument(!tables.isEmpty());
        this.tables = tables;
    }

    private GenerationId getGenerationId() throws InterruptedException, ExecutionException {
        Optional<GenerationId> generationId = transport.getCurrentGenerationId();
        if (generationId.isPresent()) {
            return generationId.get();
        }
        while (true) {
            generationId = cql.fetchFirstGenerationId().get();
            if (generationId.isPresent()) {
                return generationId.get();
            }
            Thread.sleep(SLEEP_BEFORE_FIRST_GENERATION_MS);
        }
    }

    private boolean generationDone(GenerationMetadata generation, Set<TaskId> tasks) {
        return generation.isClosed() && !tasks.isEmpty() && transport.areTasksFullyConsumedUntil(tasks, generation.getEnd().get());
    }

    private GenerationMetadata getNextGeneration(GenerationMetadata generation)
            throws InterruptedException, ExecutionException {
        return cql.fetchGenerationMetadata(generation.getNextGenerationId().get()).get();
    }

    private Map<TaskId, SortedSet<StreamId>> createTasks(GenerationMetadata generation) {
        SortedSet<StreamId> streams = generation.getStreams();
        Map<TaskId, SortedSet<StreamId>> tasks = new HashMap<>();
        for (StreamId s : streams) {
            for (TableName t : tables) {
                TaskId taskId = new TaskId(generation.getId(), s.getVNodeId(), t);
                tasks.computeIfAbsent(taskId, id -> new TreeSet<>()).add(s);
            }
        }
        return tasks;
    }

    private GenerationMetadata refreshEnd(GenerationMetadata generation)
            throws InterruptedException, ExecutionException {
        Optional<Timestamp> end = cql.fetchGenerationEnd(generation.getId()).get();
        return end.isPresent() ? generation.withEnd(end.get()) : generation;
    }

    public void run() {
        // Until the master thread is interrupted, continuously run fetching
        // the new generations. In case of exception (for example
        // CQL query error), infinitely retry the master routine from
        // the beginning, upon waiting with a fixed backoff time.
        while (!Thread.interrupted()) {
            try {
                runUntilException();
            } catch (Exception ex) {
                logger.atSevere().withCause(ex).log("Got an Exception inside Master. " +
                        "Will attempt to retry after a back-off time.");
            }
            // Retry backoff
            try {
                Thread.sleep(SLEEP_AFTER_EXCEPTION_MS);
            } catch (InterruptedException e) {
                // Interruptions are expected.
                Thread.currentThread().interrupt();
            }
        }
    }

    private void runUntilException() throws ExecutionException {
        try {
            GenerationId generationId = getGenerationId();
            GenerationMetadata generation = cql.fetchGenerationMetadata(generationId).get();
            Map<TaskId, SortedSet<StreamId>> tasks = createTasks(generation);
            while (!Thread.interrupted()) {
                while (generationDone(generation, tasks.keySet())) {
                    generation = getNextGeneration(generation);
                    tasks = createTasks(generation);
                }
                transport.configureWorkers(tasks);
                while (!generationDone(generation, tasks.keySet())) {
                    Thread.sleep(SLEEP_BEFORE_GENERATION_DONE_MS);
                    if (!generation.isClosed()) {
                        generation = refreshEnd(generation);
                    }
                }
            }
        } catch (InterruptedException e) {
            // Interruptions are expected.
            Thread.currentThread().interrupt();
        }
    }

}
