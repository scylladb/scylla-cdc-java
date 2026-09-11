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

    /**
     * Loads states for tasks which are not assigned to this worker, solely for migrating their
     * checkpoints to assigned tasks.
     *
     * <p>This separate method prevents migration lookups from violating the assignment assumptions
     * of {@link #getTaskStates(Set)}. Transports must override it before running tablet task
     * assignments. Stateless transports which cannot have a legacy checkpoint may return an empty
     * map; persistent transports must look up the requested unassigned checkpoints. The default
     * fails fast rather than silently replaying retained CDC history.
     */
    default Map<TaskId, TaskState> getTaskStatesForMigration(Set<TaskId> tasks) {
        throw new UnsupportedOperationException(
                "WorkerTransport must override getTaskStatesForMigration before running tablet "
                        + "task assignments");
    }

    /**
     * Called after replacement task states have been stored successfully for every task prepared
     * by the worker. A persistent transport can use this callback to retire the returned legacy
     * checkpoints. A transport which partitions one legacy task across multiple workers must
     * coordinate completion across all replacements before deleting it. The default is a no-op for
     * transports which do not persist task state.
     *
     * @param legacyTasks legacy task IDs returned by {@link #getTaskStatesForMigration(Set)}
     */
    default void completeTaskStateMigration(Set<TaskId> legacyTasks) {
    }

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
