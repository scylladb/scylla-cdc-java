package com.scylladb.cdc.transport;

import java.util.Map;
import java.util.Set;

import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.TaskState;

/*
 * It is safe to assume that this interface will be called from a single thread only
 */
public interface WorkerTransport {
    Map<TaskId, TaskState> getTaskStates(Set<TaskId> tasks);
    void setState(TaskId task, TaskState newState);
    void moveStateToNextWindow(TaskId task, TaskState newState);

    /**
     * @deprecated Use {@link Worker.stop()} instead")
     * @return
     */
    @Deprecated
    default boolean shouldStop() {
        return false;
    }
}
