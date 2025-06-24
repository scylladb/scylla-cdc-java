package com.scylladb.cdc.model.master;

import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class GenerationBasedCDCMetadataModel implements CDCMetadataModel {

    private final MasterConfiguration masterConfiguration;
    private GenerationMetadata generationMetadata;
    private final Set<TableName> tables;
    private Map<TaskId, SortedSet<StreamId>> tasks;

    public GenerationBasedCDCMetadataModel(GenerationMetadata generationMetadata, Set<TableName> tables, MasterConfiguration masterConfiguration) {
        this.masterConfiguration = masterConfiguration;
        this.tables = Collections.unmodifiableSet(new HashSet<>(tables));
        this.generationMetadata = generationMetadata;
        this.tasks = Collections.unmodifiableMap(createTasks(generationMetadata, this.tables));
    }

    /**
     * Sets the generation metadata and updates tasks accordingly.
     */
    private void setGenerationMetadata(GenerationMetadata generationMetadata) {
        this.generationMetadata = generationMetadata;
        this.tasks = Collections.unmodifiableMap(createTasks(generationMetadata, this.tables));
    }

    /**
     * Updates this model to the next generation in-place.
     * Returns true if successful, false if there is no next generation.
     */
    public void advanceToNextGeneration() throws InterruptedException, ExecutionException {
        GenerationMetadata nextGen = getNextGeneration(this.getGenerationMetadata());
        setGenerationMetadata(nextGen);
    }

    private static Map<TaskId, SortedSet<StreamId>> createTasks(GenerationMetadata generation, Set<TableName> tables) {
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

    public GenerationMetadata getGenerationMetadata() {
        return generationMetadata;
    }

    private static GenerationId getGenerationId(MasterConfiguration masterConfiguration)
            throws InterruptedException, ExecutionException {
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

    private GenerationMetadata getNextGeneration(
            GenerationMetadata generation
    ) throws InterruptedException, ExecutionException {
        return masterConfiguration.cql.fetchGenerationMetadata(generation.getNextGenerationId().get()).get();
    }

    private boolean generationDone(
            GenerationMetadata generation,
            Set<TaskId> tasks
    ) throws ExecutionException, InterruptedException {
        if (!generation.isClosed()) {
            return false;
        }
        if (generationTTLExpired(generation)) {
            return true;
        }
        return masterConfiguration.transport.areTasksFullyConsumedUntil(tasks, generation.getEnd().get());
    }

    private boolean generationTTLExpired(
            GenerationMetadata generation
    ) throws ExecutionException, InterruptedException {
        Date now = Date.from(masterConfiguration.clock.instant());
        List<Optional<Long>> tablesTTL = new ArrayList<>();
        for (TableName table : masterConfiguration.tables) {
            Optional<Long> ttl = masterConfiguration.cql.fetchTableTTL(table).exceptionally(ex -> {
                FluentLogger.forEnclosingClass().atSevere().withCause(ex).log("Error while fetching TTL value for table %s.%s", table.keyspace, table.name);
                return Optional.empty();
            }).get();
            tablesTTL.add(ttl);
        }
        Date lastVisibleChanges = tablesTTL.stream()
                .map(t -> t.map(ttl -> new Date(now.getTime() - 1000L * ttl)).orElse(new Date(0)))
                .min(Comparator.naturalOrder())
                .orElse(new Date(0));
        return lastVisibleChanges.after(generation.getEnd().get().toDate());
    }

    private GenerationMetadata refreshEnd(GenerationMetadata generation)
            throws InterruptedException, ExecutionException {
        Optional<Timestamp> end = masterConfiguration.cql.fetchGenerationEnd(generation.getId()).get();
        return end.isPresent() ? generation.withEnd(end.get()) : generation;
    }

    @Override
    public void runMasterStep() throws InterruptedException {
        try {
            // Advance through done generations
            while (generationDone(this.getGenerationMetadata(), this.tasks.keySet())) {
                advanceToNextGeneration();
            }

            Map<TaskId, SortedSet<StreamId>> tasks = this.tasks;

            FluentLogger.forEnclosingClass().atInfo().log("Master found a new generation: %s. Will call transport.configureWorkers().", this.getGenerationMetadata().getId());
            tasks.forEach((task, streams) ->
                    FluentLogger.forEnclosingClass().atFine().log("Created Task: %s with streams: %s", task, streams));

            masterConfiguration.transport.configureWorkers(tasks);

            // Wait for the generation to be done
            while (!generationDone(this.getGenerationMetadata(), tasks.keySet())) {
                Thread.sleep(masterConfiguration.sleepBeforeGenerationDoneMs);
                if (!this.getGenerationMetadata().isClosed()) {
                    this.generationMetadata = refreshEnd(this.getGenerationMetadata());
                }
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static CDCMetadataModel getCurrentCDCMetadataModel(MasterConfiguration masterConfiguration) throws InterruptedException, ExecutionException {
        GenerationId generationId = getGenerationId(masterConfiguration);
        GenerationMetadata generation = masterConfiguration.cql.fetchGenerationMetadata(generationId).get();
        return new GenerationBasedCDCMetadataModel(generation, masterConfiguration.tables, masterConfiguration);
    }

}
