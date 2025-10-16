package com.scylladb.cdc.transport;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.master.GenerationMetadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
     * Creates a new WorkerTasks with the given task configurations.
     *
     * @param tasks a map of task IDs to their sorted stream IDs
     * @param generationMetadata the metadata of the generation these tasks belong to
     */
    public GroupedTasks(Map<TaskId, SortedSet<StreamId>> tasks, GenerationMetadata generationMetadata) {
        Preconditions.checkNotNull(tasks, "Tasks map cannot be null");
        Preconditions.checkNotNull(generationMetadata, "Generation metadata cannot be null");
        Preconditions.checkArgument(tasks.keySet().stream().map(TaskId::getGenerationId)
            .allMatch(genId -> genId.equals(generationMetadata.getId())), "Tasks from different generations");
        this.tasks = new HashMap<>(tasks);
        this.generationMetadata = generationMetadata;
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

    @Override
    public String toString() {
        return "WorkerTasks{tasks=" + tasks + ", generationId=" + generationMetadata.getId() + '}';
    }
}
