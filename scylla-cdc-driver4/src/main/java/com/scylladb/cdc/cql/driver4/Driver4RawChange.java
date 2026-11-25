package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.cql.Cell;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;

/**
 * A CDC log row read with Java driver 4.x, the counterpart of {@code Driver3RawChange}.
 */
final class Driver4RawChange implements RawChange {
    private final Row row;
    private final ChangeSchema schema;

    Driver4RawChange(Row row, ChangeSchema schema) {
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
        return Driver4ToLibraryTranslator.translate(row.getObject(c.getIndex()), c.getCdcLogDataType());
    }

    @Override
    public boolean isNull(ChangeSchema.ColumnDefinition c) {
        return row.isNull(c.getIndex());
    }

    @Override
    public ByteBuffer getAsUnsafeBytes(ChangeSchema.ColumnDefinition c) {
        return row.getBytesUnsafe(c.getIndex());
    }

    @Override
    public Cell getCell(ChangeSchema.ColumnDefinition c) {
        return new Driver4Cell(this, c);
    }

    @Override
    public String toString() {
        return stream().map(Object::toString).collect(Collectors.joining(", ", "Driver4RawChange(", ")"));
    }
}
