package com.scylladb.cdc.replicator.operations;

import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.worker.RawChange;

import java.util.concurrent.ConcurrentHashMap;

public class RangeDeleteState {
    public static class DeletionStart {
        public final RawChange change;
        public final boolean isInclusive;

        public DeletionStart(RawChange change, boolean isInclusive) {
            this.change = change;
            this.isInclusive = isInclusive;
        }
    }

    private final ConcurrentHashMap<StreamId, DeletionStart> state = new ConcurrentHashMap<>();

    public void addStart(RawChange c, boolean inclusive) {
        state.put(c.getId().getStreamId(), new DeletionStart(c, inclusive));
    }

    public DeletionStart getStart(StreamId streamId) {
        return state.get(streamId);
    }
}
