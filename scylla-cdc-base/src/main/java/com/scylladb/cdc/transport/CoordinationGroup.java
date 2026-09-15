package com.scylladb.cdc.transport;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;

/**
 * Describes all participants which must complete one coordinated operation.
 *
 * <p>The namespace separates independent uses of the same key. The group is immutable so it can
 * be safely shared between a master, workers, and a durable {@link Coordinator} implementation.
 *
 * @param <K> coordination key type
 * @param <P> participant identifier type
 */
public final class CoordinationGroup<K, P> {
    private final String namespace;
    private final K key;
    private final Set<P> participants;

    public CoordinationGroup(String namespace, K key, Set<P> participants) {
        Preconditions.checkArgument(namespace != null && !namespace.isEmpty(),
                "Coordination namespace cannot be empty");
        this.namespace = namespace;
        this.key = Objects.requireNonNull(key, "Coordination key cannot be null");
        Objects.requireNonNull(participants, "Coordination participants cannot be null");
        Preconditions.checkArgument(!participants.isEmpty(),
                "Coordination participants cannot be empty");
        Preconditions.checkArgument(participants.stream().noneMatch(Objects::isNull),
                "Coordination participants cannot contain null");
        this.participants = Collections.unmodifiableSet(new HashSet<>(participants));
    }

    public String getNamespace() {
        return namespace;
    }

    public K getKey() {
        return key;
    }

    public Set<P> getParticipants() {
        return participants;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof CoordinationGroup)) {
            return false;
        }
        CoordinationGroup<?, ?> that = (CoordinationGroup<?, ?>) other;
        return namespace.equals(that.namespace)
                && key.equals(that.key)
                && participants.equals(that.participants);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, key, participants);
    }

    @Override
    public String toString() {
        return "CoordinationGroup{namespace='" + namespace + "', key=" + key
                + ", participants=" + participants + '}';
    }
}
