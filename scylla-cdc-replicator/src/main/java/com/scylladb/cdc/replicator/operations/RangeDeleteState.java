package com.scylladb.cdc.replicator.operations;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.TableMetadata;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.replicator.ReplicatorConsumer;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class RangeDeleteState {

    public static class PrimaryKeyValue {
        public final RawChange change;
        public final int prefixSize;

        public PrimaryKeyValue(TableMetadata table, RawChange change) {
            this.change = change;
            int prefixSize = 0;
            for (ColumnMetadata col : table.getPrimaryKey()) {
                Object colValue = change.getAsObject(col.getName());
                if (colValue != null) {
                    prefixSize++;
                } else {
                    break;
                }
            }
            this.prefixSize = prefixSize;
        }

    }

    public static class DeletionStart {
        public final PrimaryKeyValue val;
        public final boolean inclusive;

        public DeletionStart(PrimaryKeyValue v, boolean i) {
            val = v;
            inclusive = i;
        }
    }

    private static class Key {
        public final byte[] val;

        public Key(byte[] v) {
            val = v;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Key && Arrays.equals(val, ((Key) o).val);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(val);
        }
    }

    private final TableMetadata table;
    private final ConcurrentHashMap<Key, DeletionStart> state = new ConcurrentHashMap<>();

    public RangeDeleteState(TableMetadata table) {
        this.table = table;
    }

    public void addStart(RawChange c, boolean inclusive) {
        byte[] bytes = new byte[16];
        c.getId().getStreamId().getValue().duplicate().get(bytes, 0, 16);
        state.put(new Key(bytes), new DeletionStart(new PrimaryKeyValue(table, c), inclusive));
    }

    public DeletionStart getStart(byte[] streamId) {
        return state.get(new Key(streamId));
    }

    public void clear(byte[] streamId) {
        state.remove(new Key(streamId));
    }
}
