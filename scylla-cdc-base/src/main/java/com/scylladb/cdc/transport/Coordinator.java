package com.scylladb.cdc.transport;

import java.util.Set;

/**
 * Coordinates completion of operations whose participants may be split across workers.
 *
 * <p>Implementations shared by multiple workers must record progress durably and atomically.
 * Calls may be retried, so recording the same participant more than once must be idempotent. Once
 * a group completes, retries must continue returning {@code true} until the operation using the
 * coordination result has durably completed its own cleanup. Receiving different authoritative
 * participant sets for the same namespace and key is an invalid manifest and must fail.
 *
 * @param <K> coordination key type
 * @param <P> participant identifier type
 */
public interface Coordinator<K, P> {
    /**
     * Records participants completed by this caller.
     *
     * @param group authoritative complete participant set for the operation
     * @param completedParticipants participants completed by this caller; must be a subset of the
     *                              group's authoritative participants
     * @return true when every participant in the group has completed
     */
    boolean recordCompletion(CoordinationGroup<K, P> group, Set<P> completedParticipants);

    /**
     * Releases coordination progress after the operation guarded by the completed group succeeds.
     * Implementations without retained progress may keep the default no-op. Calls must be
     * idempotent.
     */
    default void finishCoordination(CoordinationGroup<K, P> group) {
    }
}
