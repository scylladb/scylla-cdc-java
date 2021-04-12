package com.scylladb.cdc.model.worker;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.function.Predicate;

import com.google.common.base.Preconditions;

/**
 * A grouping of RawChange:s comprising a single change event in CDC, i.e. the
 * CDC cql rows building up any given insert, delete, batch etc.
 * 
 * @author calle
 *
 */
public class BatchSequence implements Change {
    private final List<RawChange> changes;

    BatchSequence(List<RawChange> changes) {
        this.changes = Preconditions.checkNotNull(changes);
        Preconditions.checkElementIndex(0, changes.size(), "Must be at least one change in event");
        assert changes.get(changes.size()).isEndOfBatch();
    }

    @Override
    public ChangeId getId() {
        return changes.get(0).getId();
    }

    @Override
    public ChangeSchema getSchema() {
        return changes.get(0).getSchema();
    }

    public List<RawChange> getChanges() {
        return changes;
    }

    public List<RawChange> getPreImage() {
        return list(t -> t == RawChange.OperationType.PRE_IMAGE);
    }

    public List<RawChange> getPostImage() {
        return list(t -> t == RawChange.OperationType.POST_IMAGE);
    }

    public List<RawChange> getDelta() {
        return list(t -> {
            switch (t) {
            case PRE_IMAGE:
            case POST_IMAGE:
                return false;
            default:
                return true;
            }
        });
    }

    private List<RawChange> list(Predicate<RawChange.OperationType> p) {
        return changes.stream().filter(c -> p.test(c.getOperationType())).collect(toList());
    }

    @Override
    public int hashCode() {
        return changes.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof BatchSequence)) {
            return false;
        }
        BatchSequence other = (BatchSequence) obj;
        return changes.equals(other.changes);
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("Event").append("( id = ").append(getId());
        List<RawChange> preimage = getPreImage();
        if (!preimage.isEmpty()) {
            buf.append(
                    preimage.stream().map(Object::toString).collect(joining(", ", ", preimage = ( ", ")")));
        }
        List<RawChange> delta = getDelta();
        if (!delta.isEmpty()) {
            buf.append(delta.stream().map(Object::toString).collect(joining(", ", ", delta = ( ", ")")));
        }
        List<RawChange> postimage = getPostImage();
        if (!postimage.isEmpty()) {
            buf.append(
                    preimage.stream().map(Object::toString).collect(joining(", ", ", postimage = ( ", ")")));
        }
        buf.append(")");
        return buf.toString();
    }
}
