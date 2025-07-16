package com.scylladb.cdc.transport;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.master.GenerationMetadata;
import org.awaitility.core.ConditionFactory;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

public class MockMasterTransport implements MasterTransport {
    private volatile Timestamp currentFullyConsumedTimestamp = new Timestamp(new Date(0));
    private volatile Optional<GenerationId> currentGenerationId = Optional.empty();
    private final Map<TableName, Optional<GenerationId>> tableGenerationIds = new ConcurrentHashMap<>();
    private final List<Map<TaskId, SortedSet<StreamId>>> configureWorkersInvocations = Collections.synchronizedList(new ArrayList<>());

    // Track configureWorkers invocations per table
    private final Map<TableName, List<Map<TaskId, SortedSet<StreamId>>>> configureWorkersPerTableInvocations =
            new ConcurrentHashMap<>();

    private final AtomicInteger areTasksFullyConsumedUntilCount = new AtomicInteger(0);

    // Track generation metadata updates
    private final Map<TableName, List<GenerationMetadata>> updateGenerationMetadataInvocations = new ConcurrentHashMap<>();

    // Store only the most recent generation metadata per table
    private final Map<TableName, GenerationMetadata> tableGenerationMetadatas = new ConcurrentHashMap<>();

    // Set to store completed generation ids
    private final Set<GenerationId> completedGenerations = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // Track the last consumption timestamp for testing generation transition
    private Optional<Timestamp> lastConsumedChangeTimestamp = Optional.empty();

    /**
     * Simulates a lastConsumedChangeTimestamp for testing generation transitions.
     * @param timestamp The timestamp to set as last consumed
     */
    public void setLastConsumedChangeTimestamp(Timestamp timestamp) {
        this.lastConsumedChangeTimestamp = Optional.of(timestamp);
    }

    // Track the start and end read timestamps when configureWorkers is called for testing
    private final Map<GenerationId, Timestamp> generationStartReadTimestamps = new HashMap<>();
    private final Map<GenerationId, Optional<Timestamp>> generationEndReadTimestamps = new HashMap<>();

    /**
     * Gets the start read timestamp used to configure a specific generation
     * @param generationId The generation ID
     * @return The timestamp or null if not found
     */
    public Timestamp getGenerationStartReadTimestamp(GenerationId generationId) {
        return generationStartReadTimestamps.get(generationId);
    }

    /**
     * Gets the end read timestamp used to configure a specific generation
     * @param generationId The generation ID
     * @return The timestamp or null if not found
     */
    public Timestamp getGenerationEndReadTimestamp(GenerationId generationId) {
        return generationEndReadTimestamps.get(generationId).orElse(null);
    }

    public void setCurrentFullyConsumedTimestamp(Timestamp newTimestamp) {
        currentFullyConsumedTimestamp = Preconditions.checkNotNull(newTimestamp);
    }

    public void setGenerationFullyConsumed(GenerationMetadata generation) {
        Preconditions.checkNotNull(generation);

        // Set currentFullyConsumedTimestamp well past generation end.
        currentFullyConsumedTimestamp = generation.getEnd().get().plus(1, ChronoUnit.MINUTES);
    }

    public void setCurrentGenerationId(Optional<GenerationId> newGenerationId) {
        currentGenerationId = Preconditions.checkNotNull(newGenerationId);
    }

    public void setCurrentGenerationId(TableName tableName, Optional<GenerationId> generationId) {
        tableGenerationIds.put(tableName, generationId);
    }

    public Map<TaskId, SortedSet<StreamId>> getConfigureWorkersInvocation(int index) {
        if (index >= configureWorkersInvocations.size()) {
            return null;
        }
        return configureWorkersInvocations.get(index);
    }

    public Map<TaskId, SortedSet<StreamId>> getConfigureWorkersInvocation(TableName tableName, int index) {
        List<Map<TaskId, SortedSet<StreamId>>> tableInvocations = configureWorkersPerTableInvocations.get(tableName);
        if (tableInvocations == null || index >= tableInvocations.size()) {
            return null;
        }
        return tableInvocations.get(index);
    }

    public int getConfigureWorkersInvocationsCount() {
        return configureWorkersInvocations.size();
    }

    public int getConfigureWorkersInvocationsCount(TableName tableName) {
        List<Map<TaskId, SortedSet<StreamId>>> tableInvocations = configureWorkersPerTableInvocations.get(tableName);
        return tableInvocations != null ? tableInvocations.size() : 0;
    }

    public int getAreTasksFullyConsumedUntilCount() {
        return areTasksFullyConsumedUntilCount.get();
    }

    public ConfigureWorkersTracker tracker(ConditionFactory await) {
        return new ConfigureWorkersTracker(this, await);
    }

    @Override
    public Optional<GenerationId> getCurrentGenerationId() {
        return currentGenerationId;
    }

    @Override
    public boolean areTasksFullyConsumedUntil(Set<TaskId> tasks, Timestamp until) {
        areTasksFullyConsumedUntilCount.incrementAndGet();
        return until.compareTo(currentFullyConsumedTimestamp) < 0;
    }

    @Override
    public void configureWorkers(GroupedTasks workerTasks) throws InterruptedException {
        configureWorkersInvocations.add(workerTasks.getTasks());

        // For testing generation transition, capture the startReadTimestamp and endReadTimestamp
        GenerationId genId = workerTasks.getGenerationId();
        generationStartReadTimestamps.put(genId, workerTasks.getStartReadTimestamp());
        generationEndReadTimestamps.put(genId, workerTasks.getEndReadTimestamp());
    }

    @Override
    public Optional<GenerationId> getCurrentGenerationId(TableName tableName) {
        return tableGenerationIds.getOrDefault(tableName, Optional.empty());
    }

    /**
     * Gets the current generation metadata for a table
     *
     * @param tableName The table name
     * @return The current generation metadata or null if not found
     */
    public GenerationMetadata getCurrentGenerationMetadata(TableName tableName) {
        return tableGenerationMetadatas.get(tableName);
    }

    @Override
    public void configureWorkers(TableName tableName, GroupedTasks workerTasks)
            throws InterruptedException {
        // Add to general invocations list
        configureWorkersInvocations.add(workerTasks.getTasks());

        // Add to per-table invocations map
        configureWorkersPerTableInvocations.computeIfAbsent(tableName,
                t -> Collections.synchronizedList(new ArrayList<>()))
                .add(workerTasks.getTasks());

        // Update the current generation ID for this table
        GenerationId genId = workerTasks.getGenerationId();
        tableGenerationIds.put(tableName, Optional.of(genId));

        // Store the generation metadata (only most recent)
        tableGenerationMetadatas.put(tableName, workerTasks.getGenerationMetadata());

        // For testing generation transition, capture the startReadTimestamp and endReadTimestamp
        generationStartReadTimestamps.put(genId, workerTasks.getStartReadTimestamp());
        generationEndReadTimestamps.put(genId, workerTasks.getEndReadTimestamp());
    }

    @Override
    public void updateGenerationMetadata(TableName table, GenerationMetadata metadata) {
        // Store the generation metadata in our tracking map
        updateGenerationMetadataInvocations.computeIfAbsent(table, t -> Collections.synchronizedList(new ArrayList<>()))
                .add(metadata);

        // Update the current metadata for this table
        tableGenerationMetadatas.put(table, metadata);
    }

    /**
     * Sets a generation as completed.
     * @param generationId The generation ID to mark as completed.
     */
    public void setGenerationCompleted(GenerationId generationId) {
        Preconditions.checkNotNull(generationId);
        completedGenerations.add(generationId);
    }

    /**
     * Checks if a specific generation is marked as completed.
     * @param generationId The generation ID to check.
     * @return true if the generation is completed, false otherwise.
     */
    public boolean isGenerationCompleted(GenerationId generationId) {
        return completedGenerations.contains(generationId);
    }

    @Override
    public boolean areTasksCompleted(Set<TaskId> tasks) {
        areTasksFullyConsumedUntilCount.incrementAndGet();

        // If there are no tasks, consider them completed
        if (tasks == null || tasks.isEmpty()) {
            return true;
        }

        for (TaskId taskId : tasks) {
            // Check if the generation of this task is completed
            GenerationId genId = taskId.getGenerationId();
            if (!completedGenerations.contains(genId)) {
                return false;
            }
        }

        // All tasks are either before currentFullyConsumedTimestamp or their generation is completed
        return true;
    }

    @Override
    public Optional<Timestamp> getLastConsumedChangeTimestamp(Set<TaskId> tasks) {
        return lastConsumedChangeTimestamp;
    }

}
