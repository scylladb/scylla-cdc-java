package com.scylladb.cdc.transport;

import java.util.Map;
import java.util.Set;

import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.TaskState;

public interface WorkerTransport {
    Map<TaskId, TaskState> getTaskStates(Set<TaskId> tasks);
    void setState(TaskId task, TaskState newState);
    void moveStateToNextWindow(TaskId task, TaskState newState);
    boolean shouldStop();
}
