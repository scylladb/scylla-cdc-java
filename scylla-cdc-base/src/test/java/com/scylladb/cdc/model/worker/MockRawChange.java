package com.scylladb.cdc.model.worker;

import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.model.worker.cql.Field;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MockRawChange implements RawChange {
    private final ChangeSchema changeSchema;
    private final Map<String, Object> columnValues;

    public MockRawChange(ChangeSchema changeSchema, Map<String, Object> columnValues) {
        this.changeSchema = changeSchema;
        this.columnValues = columnValues;
    }

    @Override
    public ChangeSchema getSchema() {
        return changeSchema;
    }

    @Override
    public Object getAsObject(ChangeSchema.ColumnDefinition c) {
        return columnValues.get(c.getColumnName());
    }

    @Override
    public Cell getCell(ChangeSchema.ColumnDefinition c) {
        return new Cell() {
            @Override
            public ChangeSchema.ColumnDefinition getColumnDefinition() {
                return c;
            }

            @Override
            public Set<Field> getDeletedElements() {
                throw new UnsupportedOperationException("Not implemented yet");
            }

            @Override
            public boolean hasDeletedElements() {
                throw new UnsupportedOperationException("Not implemented yet");
            }

            @Override
            public boolean isDeleted() {
                throw new UnsupportedOperationException("Not implemented yet");
            }

            @Override
            public ByteBuffer getAsUnsafeBytes() {
                throw new UnsupportedOperationException("Not implemented yet");
            }

            @Override
            public Object getAsObject() {
                return MockRawChange.this.getAsObject(c);
            }

            @Override
            public ChangeSchema.DataType getDataType() {
                return c.getCdcLogDataType();
            }
        };
    }

    @Override
    public boolean isNull(ChangeSchema.ColumnDefinition c) {
        return getAsObject(c) == null;
    }

    @Override
    public ByteBuffer getAsUnsafeBytes(ChangeSchema.ColumnDefinition c) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MockRawChange cells = (MockRawChange) o;
        return changeSchema.equals(cells.changeSchema) &&
                columnValues.equals(cells.columnValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(changeSchema, columnValues);
    }
}
