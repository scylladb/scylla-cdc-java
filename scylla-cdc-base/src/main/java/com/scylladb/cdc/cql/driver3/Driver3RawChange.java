package com.scylladb.cdc.cql.driver3;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Row;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeSchema;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public final class Driver3RawChange implements RawChange {
    private final Row row;
    private final Driver3ChangeSchema schema;

    public Driver3RawChange(Row row, Driver3ChangeSchema schema) {
        this.row = Preconditions.checkNotNull(row);
        this.schema = Preconditions.checkNotNull(schema);
    }

    @Override
    public ChangeId getId() {
        return new ChangeId(new StreamId(row.getBytes(quoteIfNecessary("cdc$stream_id"))),
                row.getUUID(quoteIfNecessary("cdc$time")));
    }

    @Override
    public ChangeSchema getSchema() {
        return schema;
    }

    @Override
    public Object getAsObject(String columnName) {
        if (row.isNull(columnName)) {
            return null;
        } else {
            // TODO - just use getObject()... - drastically simplify!
            TypeToken<Object> type = CodecRegistry.DEFAULT_INSTANCE.codecFor(schema.getDriverType(columnName)).getJavaType();
            return row.get(columnName, type);
        }
    }

    @Override
    public ByteBuffer getAsBytes(String columnName) {
        if (row.isNull(columnName)) {
            return null;
        } else {
            return row.getBytesUnsafe(columnName);
        }
    }

    /*
     * What follows are temporary methods
     * used for porting the replicator
     * from old library to new library.
     *
     * Those methods should be removed
     * after the porting process is done.
     */

    @Override
    public boolean TEMPORARY_PORTING_isDeleted(String name) {
        String deletionColumnName = "cdc$deleted_" + name;
        return !row.isNull(deletionColumnName) && row.getBool(deletionColumnName);
    }

    @Override
    public Row TEMPORARY_PORTING_row() {
        return row;
    }
}
