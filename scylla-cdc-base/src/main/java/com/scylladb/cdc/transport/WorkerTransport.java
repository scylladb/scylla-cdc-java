package com.scylladb.cdc.transport;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.TaskState;
import com.scylladb.cdc.model.worker.Worker;

/*
 * It is safe to assume that this interface will be called from a single thread only
 */
public interface WorkerTransport extends Coordinator<TaskId, TaskId> {
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
     * Records completion without retaining progress between calls.
     *
     * <p>This default safely completes a coordination only when one worker reports the complete
     * authoritative participant set. A transport which partitions a coordination group across
     * workers may override this method with shared, durable, and atomic progress tracking.
     */
    @Override
    default boolean recordCompletion(CoordinationGroup<TaskId, TaskId> group,
                                     Set<TaskId> completedParticipants) {
        Objects.requireNonNull(group, "Coordination group cannot be null");
        Objects.requireNonNull(completedParticipants,
                "Completed coordination participants cannot be null");
        if (!group.getParticipants().containsAll(completedParticipants)) {
            throw new IllegalArgumentException(
                    "Completed participants must belong to the coordination group");
        }
        return completedParticipants.containsAll(group.getParticipants());
    }

    /**
     * Called after replacement task states have been stored and their coordination group has
     * completed according to {@link #recordCompletion(CoordinationGroup, Set)}. A persistent
     * transport can use this callback to retire the returned legacy checkpoints. Calls may be
     * retried, so retirement must be idempotent. The default is a no-op for transports which do not
     * persist task state.
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
