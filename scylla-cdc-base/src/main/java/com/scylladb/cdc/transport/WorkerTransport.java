package com.scylladb.cdc.transport;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.master.GenerationMetadata;
import com.scylladb.cdc.model.worker.TaskState;
import com.scylladb.cdc.model.worker.Worker;

/*
 * It is safe to assume that this interface will be called from a single thread only
 */
public interface WorkerTransport {
    Map<TaskId, TaskState> getTaskStates(Set<TaskId> tasks);
    void setState(TaskId task, TaskState newState);
    void moveStateToNextWindow(TaskId task, TaskState newState);

    /**
     * @deprecated Use {@link Worker#stop()} instead")
     * @return
     */
    @Deprecated
    default boolean shouldStop() {
        return false;
    }

    /**
     * Gets the end timestamp for tasks related to the given table, if set.
     * @return Optional containing the end timestamp if set, empty Optional otherwise
     */
    public Optional<Timestamp> getTableEndTimestamp(TableName table);

}
