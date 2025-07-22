package com.scylladb.cdc.model.worker;

import java.util.Objects;
import java.util.SortedSet;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;

public final class Task {
    public final TaskId id;
    public final SortedSet<StreamId> streams;
    public final TaskState state;

    public Task(TaskId id, SortedSet<StreamId> streams, TaskState state) {
        this.id = Preconditions.checkNotNull(id);
        this.streams = Preconditions.checkNotNull(streams);
        this.state = Preconditions.checkNotNull(state);
    }

    public Task updateState(TaskState newState) {
        Preconditions.checkNotNull(newState);
        return new Task(id, streams, newState);
    }

    public Task updateState(ChangeId lastSeenChangeId) {
        Preconditions.checkNotNull(lastSeenChangeId);
        return new Task(id, streams, state.update(lastSeenChangeId));
    }

    /**
     * Updates the task with an end timestamp.
     *
     * @param endTimestamp The timestamp beyond which this task should not read
     * @return A new Task with updated state containing the end timestamp
     */
    public Task withEndTimestamp(Timestamp endTimestamp) {
        Preconditions.checkNotNull(endTimestamp);
        return new Task(id, streams, state.withEndTimestamp(endTimestamp));
    }

    /**
     * Checks if the task has reached its defined end timestamp (if any)
     *
     * @return true if the task has reached its end timestamp
     */
    public boolean hasReachedEnd() {
        return state.hasReachedEnd();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Task)) return false;
        Task task = (Task) o;
        return id.equals(task.id) &&
                streams.equals(task.streams) &&
                state.equals(task.state);
    }

    @Override
    public String toString() {
        return String.format("Task(%s, %s, %s)", id, streams, state);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, streams, state);
    }
}
