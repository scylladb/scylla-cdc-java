package com.scylladb.cdc.model.master;

import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.model.CatchUpUtils;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.transport.GroupedTasks;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Controller for CDC logic of a single table in tablet-based model.
 */
public class TableCDCController {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final TableName table;
    private final MasterConfiguration masterConfiguration;
    private GenerationMetadata currentGeneration;
    private GroupedTasks tasks;

    public TableCDCController(TableName table, MasterConfiguration masterConfiguration) {
        this.table = table;
        this.masterConfiguration = masterConfiguration;
        this.tasks = null;
    }

    private static GenerationId getGenerationId(TableName table, MasterConfiguration masterConfiguration) throws ExecutionException, InterruptedException {
        // If we already have a current generation stored in the transport state, return it, and
        // otherwise fetch the first generation.
        Optional<GenerationId> generationId = masterConfiguration.transport.getCurrentGenerationId(table);
        if (generationId.isPresent()) {
            return generationId.get();
        }

        // If catch-up is enabled, try to jump directly to a recent generation.
        Optional<GenerationId> jumpTarget = CatchUpUtils.tryJumpToRecentGeneration(
                masterConfiguration.computeCatchUpCutoff(),
                cutoff -> masterConfiguration.cql.fetchLastTableGenerationBeforeOrAt(table, cutoff),
                Optional.of(table));
        if (jumpTarget.isPresent()) {
            return jumpTarget.get();
        }

        return masterConfiguration.cql.fetchFirstTableGenerationId(table).get();
    }

    public void initCurrentGeneration() throws InterruptedException, ExecutionException {
        // Initialize the current generation metadata for this table.
        GenerationId generationId = getGenerationId(table, masterConfiguration);
        this.currentGeneration = masterConfiguration.cql.fetchTableGenerationMetadata(table, generationId).get();
        this.tasks = createTasks(currentGeneration, table);
        logger.atInfo().log("Initialized current generation for table %s with ID %s.", table, generationId);

        while (generationDone()) {
            advanceToNextGeneration();
        }

        configureWorkers();
    }

    public void advanceToNextGeneration() throws InterruptedException, ExecutionException {
        Optional<GenerationId> nextGenId = currentGeneration.getNextGenerationId();
        if (!nextGenId.isPresent()) {
            throw new IllegalStateException("No next generation available for table: " + table);
        }

        this.currentGeneration = masterConfiguration.cql.fetchTableGenerationMetadata(table, nextGenId.get()).get();
        this.tasks = createTasks(currentGeneration, table);
        logger.atInfo().log("Master found a new generation for table %s with ID %s.", table, nextGenId.get());
    }

    private static GroupedTasks createTasks(GenerationMetadata generation, TableName table) {
        SortedSet<StreamId> streams = generation.getStreams();
        Map<TaskId, SortedSet<StreamId>> taskMap = new HashMap<>();
        for (StreamId s : streams) {
            TaskId taskId = new TaskId(generation.getId(), s.getVNodeId(), table);
            taskMap.computeIfAbsent(taskId, id -> new TreeSet<>()).add(s);
        }
        return new GroupedTasks(taskMap, generation);
    }

    /**
     * Checks if the generation TTL has expired for this table.
     * If TTL has expired, changes from this generation are no longer visible.
     *
     * @return true if the TTL has expired, false otherwise
     * @throws ExecutionException if there's an error fetching TTL value
     * @throws InterruptedException if the operation is interrupted
     */
    private boolean generationTTLExpired() throws ExecutionException, InterruptedException {
        Date now = Date.from(masterConfiguration.clock.instant());

        // Get the TTL for this table
        Optional<Long> ttl = masterConfiguration.cql.fetchTableTTL(table).exceptionally(ex -> {
            logger.atSevere().withCause(ex).log("Error while fetching TTL value for table %s.%s",
                    table.keyspace, table.name);
            return Optional.empty();
        }).get();

        // If no TTL, then changes never expire
        if (!ttl.isPresent()) {
            return false;
        }

        Date lastVisibleChanges = new Date(now.getTime() - 1000L * ttl.get());
        return lastVisibleChanges.after(currentGeneration.getEnd()
                .orElseThrow(() -> new IllegalStateException("Expected closed generation to have an end timestamp"))
                .toDate());
    }

    /**
     * Checks if the current generation is done (closed and all tasks are fully consumed).
     *
     * @return true if the current generation is done, false otherwise
     */
    public boolean generationDone() throws InterruptedException, ExecutionException {
        if (!currentGeneration.isClosed()) {
            return false;
        }

        if (generationTTLExpired()) {
            return true;
        }

        // Otherwise check if all tasks are completed
        return masterConfiguration.transport.areTasksFullyConsumedUntil(tasks.getTasks().keySet(),
                currentGeneration.getEnd()
                        .orElseThrow(() -> new IllegalStateException("Expected closed generation to have an end timestamp")));
    }

    /**
     * Refreshes the end timestamp of the current generation
     *
     * @return true if end timestamp was refreshed, false otherwise
     */
    public boolean refreshEnd() throws InterruptedException, ExecutionException {
        if (currentGeneration.isClosed()) {
            return false;
        }

        Optional<Timestamp> endTimestamp = masterConfiguration.cql.fetchTableGenerationEnd(this.table, currentGeneration.getId()).get();
        if (endTimestamp.isPresent()) {
            Timestamp end = endTimestamp.get();
            currentGeneration = currentGeneration.withEnd(end);
            logger.atFine().log("Updated end timestamp for table %s generation %s to %s",
                    table, currentGeneration.getId(), end);
            return true;
        }
        return false;
    }

    /**
     * Configures workers for the current generation tasks.
     *
     * @throws InterruptedException
     */
    public void configureWorkers() throws InterruptedException {
        logger.atFine().log("Configuring workers for table %s: %s",
                table, currentGeneration.getId());
        tasks.getTasks().forEach((task, streams) -> logger.atFine().log("Created Task: %s with streams: %s", task, streams));

        masterConfiguration.transport.configureWorkers(this.table, tasks);
    }

    public void runMasterStep() throws InterruptedException, ExecutionException {
        refreshEnd();

        if (generationDone()) {
            advanceToNextGeneration();
            configureWorkers();
        }
    }
}
