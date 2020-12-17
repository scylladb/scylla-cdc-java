package com.scylladb.cdc.replicator.operations.delete;

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

    public void addStart(RawChange c, boolean isInclusive) {
        // This method is called by RangeDeleteStartOperationHandler
        // to store the row range delete start for
        // later use when the row range delete end is known and
        // the row range delete can be then performed.

        state.put(c.getId().getStreamId(), new DeletionStart(c, isInclusive));
    }

    public DeletionStart consumeStart(StreamId streamId) {
        // This method is called by RangeDeleteEndOperationHandler
        // to retrieve the start of row range delete. We
        // remove it from map, because it will be
        // no longer needed (next row range delete
        // will add another start).

        return state.remove(streamId);
    }
}
