package com.scylladb.cdc.model.worker;

import java.util.SortedSet;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;

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
}
