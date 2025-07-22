package com.scylladb.cdc.transport;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.master.GenerationMetadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;

/**
 * Represents a set of tasks that are created together within the same generation and share
 * common generation metadata.
 */
public class GroupedTasks {
    private final Map<TaskId, SortedSet<StreamId>> tasks;
    private final GenerationMetadata generationMetadata;

    /**
     * The timestamp from which to start reading changes.
     * If not set, defaults to the generation start timestamp.
     * This allows overriding the start point for reading when needed,
     * for example when resuming from a specific point.
     */
    private Optional<Timestamp> startReadTimestamp;

    /**
     * The timestamp at which to stop reading changes.
     * If not set, there is no predefined end point and reading can continue indefinitely
     * until a closed time is encountered or the worker is stopped.
     * Unlike startReadTimestamp, this has no default value from the generation.
     */
    private Optional<Timestamp> endReadTimestamp;

    /**
     * Creates a new WorkerTasks with the given task configurations.
     *
     * @param tasks a map of task IDs to their sorted stream IDs
     * @param generationMetadata the metadata of the generation these tasks belong to
     */
    public GroupedTasks(Map<TaskId, SortedSet<StreamId>> tasks, GenerationMetadata generationMetadata) {
        this(tasks, generationMetadata, Optional.empty(), Optional.empty());
    }

    /**
     * Creates a new WorkerTasks with the given task configurations and timestamp boundaries.
     *
     * @param tasks a map of task IDs to their sorted stream IDs
     * @param generationMetadata the metadata of the generation these tasks belong to
     * @param startReadTimestamp optional timestamp to start reading from
     * @param endReadTimestamp optional timestamp to end reading at
     */
    public GroupedTasks(Map<TaskId, SortedSet<StreamId>> tasks, GenerationMetadata generationMetadata,
                        Optional<Timestamp> startReadTimestamp, Optional<Timestamp> endReadTimestamp) {
        Preconditions.checkNotNull(tasks, "Tasks map cannot be null");
        Preconditions.checkNotNull(generationMetadata, "Generation metadata cannot be null");
        Preconditions.checkNotNull(startReadTimestamp, "Start read timestamp cannot be null");
        Preconditions.checkNotNull(endReadTimestamp, "End read timestamp cannot be null");
        Preconditions.checkArgument(tasks.keySet().stream().map(TaskId::getGenerationId)
            .allMatch(genId -> genId.equals(generationMetadata.getId())), "Tasks from different generations");

        this.tasks = new HashMap<>(tasks);
        this.generationMetadata = generationMetadata;
        this.startReadTimestamp = startReadTimestamp;
        this.endReadTimestamp = endReadTimestamp;
    }

    /**
     * Returns the underlying task map.
     *
     * @return an unmodifiable view of the task map
     */
    public Map<TaskId, SortedSet<StreamId>> getTasks() {
        return Collections.unmodifiableMap(tasks);
    }

    /**
     * Returns the set of task IDs.
     *
     * @return the set of task IDs
     */
    public Set<TaskId> getTaskIds() {
        return Collections.unmodifiableSet(tasks.keySet());
    }

    /**
     * Returns the stream IDs for a specific task.
     *
     * @param taskId the task ID
     * @return the sorted set of stream IDs for the task, or null if the task doesn't exist
     */
    public SortedSet<StreamId> getStreamsForTask(TaskId taskId) {
        return tasks.get(taskId);
    }

    /**
     * Returns the number of tasks.
     *
     * @return the number of tasks
     */
    public int size() {
        return tasks.size();
    }

    /**
     * Returns the generation metadata for this worker tasks.
     *
     * @return the generation metadata
     */
    public GenerationMetadata getGenerationMetadata() {
        return generationMetadata;
    }

    /**
     * Returns the generation ID for this worker tasks.
     *
     * @return the generation ID
     */
    public GenerationId getGenerationId() {
        return generationMetadata.getId();
    }

    /**
     * Returns the start read timestamp that defines the beginning of data to read.
     * If startReadTimestamp is not explicitly set, returns the generation start timestamp.
     *
     * @return the start read timestamp
     */
    public Timestamp getStartReadTimestamp() {
        return startReadTimestamp.orElseGet(() -> generationMetadata.getStart());
    }

    /**
     * Sets the start read timestamp that defines the beginning of data to read.
     *
     * @param startReadTimestamp the optional timestamp to start reading from
     * @return this instance for method chaining
     */
    public GroupedTasks withStartReadTimestamp(Optional<Timestamp> startReadTimestamp) {
        Preconditions.checkNotNull(startReadTimestamp, "Start read timestamp cannot be null");
        return new GroupedTasks(this.tasks, this.generationMetadata, startReadTimestamp, this.endReadTimestamp);
    }

    /**
     * Returns the end read timestamp that defines the end of data to read.
     *
     * Unlike the start timestamp which is always present (either explicitly set or derived from
     * the generation start), the end timestamp is optional. An empty end timestamp means
     * reading should continue indefinitely or until a closed time is encountered.
     *
     * @return the optional end read timestamp
     */
    public Optional<Timestamp> getEndReadTimestamp() {
        return endReadTimestamp;
    }

    /**
     * Sets the end read timestamp that defines the end of data to read.
     *
     * @param endReadTimestamp the optional timestamp to end reading at
     * @return this instance for method chaining
     */
    public GroupedTasks withEndReadTimestamp(Optional<Timestamp> endReadTimestamp) {
        Preconditions.checkNotNull(endReadTimestamp, "End read timestamp cannot be null");
        return new GroupedTasks(this.tasks, this.generationMetadata, this.startReadTimestamp, endReadTimestamp);
    }

    @Override
    public String toString() {
        return "WorkerTasks{tasks=" + tasks +
               ", generationId=" + generationMetadata.getId() +
               ", startReadTimestamp=" + startReadTimestamp +
               ", endReadTimestamp=" + endReadTimestamp +
               '}';
    }
}
