package com.scylladb.cdc.model.worker;

import java.util.Objects;
import java.util.UUID;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.StreamId;

public final class ChangeId implements Comparable<ChangeId> {
    private final StreamId streamId;
    private final ChangeTime time;

    public ChangeId(StreamId streamId, ChangeTime time) {
        this.streamId = Preconditions.checkNotNull(streamId);
        this.time = Preconditions.checkNotNull(time);
    }

    public StreamId getStreamId() {
        return streamId;
    }

    public ChangeTime getChangeTime() {
        return time;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ChangeId)) {
            return false;
        }
        ChangeId o = (ChangeId) other;
        return streamId.equals(o.streamId) && time.equals(o.time);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, time);
    }

    @Override
    public String toString() {
        return String.format("ChangeId(%s, %s)", streamId, time);
    }

    @Override
    public int compareTo(ChangeId o) {
        int cmp = streamId.compareTo(o.streamId);
        return cmp != 0 ? cmp : time.compareTo(o.time);
    }
}
