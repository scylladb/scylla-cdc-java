package com.scylladb.cdc.cql.driver4;

import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.cql.Cell;

import java.nio.ByteBuffer;

/**
 * A column of a {@link Driver4RawChange}, the counterpart of {@code Driver3Cell}.
 */
final class Driver4Cell implements Cell {
    private final Driver4RawChange change;
    private final ChangeSchema.ColumnDefinition columnDefinition;

    Driver4Cell(Driver4RawChange change, ChangeSchema.ColumnDefinition columnDefinition) {
        this.change = change;
        this.columnDefinition = columnDefinition;
    }

    @Override
    public ChangeSchema.ColumnDefinition getColumnDefinition() {
        return columnDefinition;
    }

    @Override
    public Object getAsObject() {
        return change.getAsObject(columnDefinition);
    }

    @Override
    public ChangeSchema.DataType getDataType() {
        return columnDefinition.getCdcLogDataType();
    }

    @Override
    public ByteBuffer getAsUnsafeBytes() {
        return change.getAsUnsafeBytes(columnDefinition);
    }

    @Override
    public String toString() {
        return String.format("%s = %s", columnDefinition.getColumnName(), new Driver4Field(getDataType(), getAsObject()));
    }
}
