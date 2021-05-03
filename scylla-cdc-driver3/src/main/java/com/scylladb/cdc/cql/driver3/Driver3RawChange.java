package com.scylladb.cdc.cql.driver3;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;

import com.datastax.driver.core.Row;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.ChangeSchema.ColumnDefinition;
import com.scylladb.cdc.model.worker.ChangeTime;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.cql.Cell;

public final class Driver3RawChange implements RawChange {
    private final Row row;
    private final ChangeSchema schema;

    public Driver3RawChange(Row row, ChangeSchema schema) {
        this.row = Preconditions.checkNotNull(row);
        this.schema = Preconditions.checkNotNull(schema);
    }

    @Override
    public ChangeSchema getSchema() {
        return schema;
    }

    @Override
    public Object getAsObject(ChangeSchema.ColumnDefinition c) {
        if (isNull(c)) {
            return null;
        }
        // TODO - check if quoteIfNecessary is needed here in getObject()
        return Driver3ToLibraryTranslator.translate(row.getObject(c.getIndex()), c.getCdcLogDataType());
    }

    @Override
    public boolean isNull(ColumnDefinition c) {
        return row.isNull(c.getIndex());
    }

    @Override
    public ByteBuffer getUnsafeBytes(ColumnDefinition c) {
        return row.getBytesUnsafe(c.getIndex());
    }

    @Override
    public String toString() {
        return stream().map(Object::toString).collect(Collectors.joining(", ", "Driver3RawChange(", ")"));
    }

    @Override
    public Cell getCell(ColumnDefinition c) {
        return new Driver3Cell(this, c);
    }
}
