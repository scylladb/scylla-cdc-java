package com.scylladb.cdc.transport;

import java.util.Map;
import java.util.Set;

import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.TaskState;
import com.scylladb.cdc.model.worker.Worker;

/*
 * It is safe to assume that this interface will be called from a single thread only
 */
public interface WorkerTransport {
    Map<TaskId, TaskState> getTaskStates(Set<TaskId> tasks);
    void setState(TaskId task, TaskState newState);

    /**
     * Called by a running task to update its state in the transport.
     * May throw TaskAbortedException if the task is no longer active and should abort.
     */
    void updateState(TaskId task, TaskState newState) throws TaskAbortedException;

    /**
     * Called by a running task to move its state to the next window in the transport.
     * May throw TaskAbortedException if the task is no longer active and should abort.
     */
    void moveStateToNextWindow(TaskId task, TaskState newState) throws TaskAbortedException;

    /**
     * @deprecated Use {@link Worker#stop()} instead")
     * @return
     */
    @Deprecated
    default boolean shouldStop() {
        return false;
    }
}
