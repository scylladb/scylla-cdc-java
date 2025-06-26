package com.scylladb.cdc.model.master;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.transport.GroupedTasks;

public class GenerationBasedCDCMetadataModel implements CDCMetadataModel {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final MasterConfiguration masterConfiguration;

    public GenerationBasedCDCMetadataModel(MasterConfiguration masterConfiguration) {
        this.masterConfiguration = Preconditions.checkNotNull(masterConfiguration);
    }

    private GenerationId getGenerationId() throws InterruptedException, ExecutionException {
        Optional<GenerationId> generationId = masterConfiguration.transport.getCurrentGenerationId();
        if (generationId.isPresent()) {
            return generationId.get();
        }
        while (true) {
            generationId = masterConfiguration.cql.fetchFirstGenerationId().get();
            if (generationId.isPresent()) {
                return generationId.get();
            }
            Thread.sleep(masterConfiguration.sleepBeforeFirstGenerationMs);
        }
    }

    private boolean generationDone(GenerationMetadata generation, Set<TaskId> tasks) throws ExecutionException, InterruptedException {
        if (!generation.isClosed()) {
            return false;
        }

        if (generationTTLExpired(generation)) {
            return true;
        }

        return masterConfiguration.transport.areTasksFullyConsumedUntil(tasks, generation.getEnd().get());
    }

    private boolean generationTTLExpired(GenerationMetadata generation) throws ExecutionException, InterruptedException {
        // Check the CDC tables TTL values.
        //
        // By default the TTL value is relatively
        // small (24 hours), which means that we
        // could safely skip some older generations
        // (the changes in them have already
        // expired).
        Date now = Date.from(masterConfiguration.clock.instant());
        List<Optional<Long>> tablesTTL = new ArrayList<>();
        for (TableName table : masterConfiguration.tables) {
            // In case fetching the TTL value was unsuccessful,
            // assume that no TTL is set on a table. This way
            // the generation will not expire. By "catching"
            // the exception here, one "bad" table will
            // not disturb the entire master process.
            Optional<Long> ttl = masterConfiguration.cql.fetchTableTTL(table).exceptionally(ex -> {
                logger.atSevere().withCause(ex).log("Error while fetching TTL " +
                        "value for table %s.%s", table.keyspace, table.name);
                return Optional.empty();
            }).get();
            tablesTTL.add(ttl);
        }

        // If tablesTTL is empty or contains a table with TTL disabled,
        // use new Date(0) value - meaning there is no lower bound
        // of row timestamps the table could possibly contain.
        Date lastVisibleChanges = tablesTTL.stream()
                // getTime() is in milliseconds, TTL is in seconds
                .map(t -> t.map(ttl -> new Date(now.getTime() - 1000L * ttl)).orElse(new Date(0)))
                .min(Comparator.naturalOrder())
                .orElse(new Date(0));

        return lastVisibleChanges.after(generation.getEnd().get().toDate());
    }

    private GenerationMetadata getNextGeneration(GenerationMetadata generation)
            throws InterruptedException, ExecutionException {
        return masterConfiguration.cql.fetchGenerationMetadata(generation.getNextGenerationId().get()).get();
    }

    private GroupedTasks createTasks(GenerationMetadata generation) {
        SortedSet<StreamId> streams = generation.getStreams();
        Map<TaskId, SortedSet<StreamId>> tasks = new HashMap<>();
        for (StreamId s : streams) {
            for (TableName t : masterConfiguration.tables) {
                TaskId taskId = new TaskId(generation.getId(), s.getVNodeId(), t);
                tasks.computeIfAbsent(taskId, id -> new TreeSet<>()).add(s);
            }
        }
        return new GroupedTasks(tasks, generation);
    }

    private GenerationMetadata refreshEnd(GenerationMetadata generation)
            throws InterruptedException, ExecutionException {
        Optional<Timestamp> end = masterConfiguration.cql.fetchGenerationEnd(generation.getId()).get();
        return end.isPresent() ? generation.withEnd(end.get()) : generation;
    }

    @Override
    public void runMasterLoop() throws InterruptedException, ExecutionException {
        GenerationId generationId = getGenerationId();
        GenerationMetadata generation = masterConfiguration.cql.fetchGenerationMetadata(generationId).get();
        GroupedTasks tasks = createTasks(generation);
        while (!Thread.currentThread().isInterrupted()) {
            while (generationDone(generation, tasks.getTaskIds())) {
                generation = getNextGeneration(generation);
                tasks = createTasks(generation);
            }

            logger.atInfo().log("Master found a new generation: %s. Will call transport.configureWorkers().", generation.getId());
            tasks.getTasks().forEach((task, streams) ->
                    logger.atFine().log("Created Task: %s with streams: %s", task, streams));

            masterConfiguration.transport.configureWorkers(tasks);
            while (!generationDone(generation, tasks.getTaskIds())) {
                Thread.sleep(masterConfiguration.sleepBeforeGenerationDoneMs);
                if (!generation.isClosed()) {
                    generation = refreshEnd(generation);
                }
            }
        }
    }
}