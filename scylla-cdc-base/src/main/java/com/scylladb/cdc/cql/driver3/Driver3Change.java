package com.scylladb.cdc.cql.driver3;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;

import com.datastax.driver.core.Row;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.worker.Change;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeSchema;

import java.util.UUID;

public final class Driver3Change implements Change {
    private final Row row;
    private final ChangeSchema schema;

    public Driver3Change(Row row, ChangeSchema schema) {
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
    public Integer getInt(String columnName) {
        if (row.isNull(columnName)) {
            return null;
        } else {
            return row.getInt(columnName);
        }
    }

    @Override
    public Byte getByte(String columnName) {
        if (row.isNull(columnName)) {
            return null;
        } else {
            return row.getByte(columnName);
        }
    }

    @Override
    public Boolean getBoolean(String columnName) {
        if (row.isNull(columnName)) {
            return null;
        } else {
            return row.getBool(columnName);
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
    public UUID TEMPORARY_PORTING_getTime() {
        return row.getUUID(quoteIfNecessary("cdc$time"));
    }

    @Override
    public Integer TEMPORARY_PORTING_getTTL() {
        return row.isNull(quoteIfNecessary("cdc$ttl")) ? null : (int)row.getLong(quoteIfNecessary("cdc$ttl"));
    }

    @Override
    public boolean TEMPORARY_PORTING_isDeleted(String name) {
        String deletionColumnName = "cdc$deleted_" + name;
        return !row.isNull(deletionColumnName) && row.getBool(deletionColumnName);
    }

    @Override
    public Row TEMPORARY_PORTING_row() {
        return row;
    }

    @Override
    public byte TEMPORARY_PORTING_getOperation() {
        return row.getByte(quoteIfNecessary("cdc$operation"));
    }
}
