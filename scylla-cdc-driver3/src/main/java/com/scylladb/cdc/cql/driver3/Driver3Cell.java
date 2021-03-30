package com.scylladb.cdc.cql.driver3;

import java.nio.ByteBuffer;
import java.util.Set;

import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.ChangeSchema.DataType;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.model.worker.cql.Field;

class Driver3Cell implements Cell {
    private final Driver3RawChange change;
    private final ChangeSchema.ColumnDefinition columnDefinition;

    public Driver3Cell(Driver3RawChange change, ChangeSchema.ColumnDefinition columnDefinition) {
        this.change = change;
        this.columnDefinition = columnDefinition;
    }

    public ChangeSchema.ColumnDefinition getColumnDefinition() {
        return columnDefinition;
    }

    @Override
    public Object getAsObject() {
        return change.getAsObject(columnDefinition);
    }

    @Override
    public DataType getDataType() {
        return columnDefinition.getCdcLogDataType();
    }
}
