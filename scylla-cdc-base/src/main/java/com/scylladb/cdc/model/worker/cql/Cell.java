package com.scylladb.cdc.model.worker.cql;

import com.scylladb.cdc.model.worker.ChangeSchema;

import java.util.Objects;

public class Cell extends AbstractField {
    private final ChangeSchema.ColumnDefinition columnDefinition;

    public Cell(ChangeSchema.ColumnDefinition columnDefinition, Object value) {
        super(value);
        this.columnDefinition = columnDefinition;
    }

    public ChangeSchema.ColumnDefinition getColumnDefinition() {
        return columnDefinition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        Cell cell = (Cell) o;
        return columnDefinition.equals(cell.columnDefinition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), columnDefinition);
    }
}
