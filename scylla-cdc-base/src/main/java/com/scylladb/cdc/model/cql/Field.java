package com.scylladb.cdc.model.cql;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.worker.ChangeSchema;

import java.util.Objects;

public class Field extends AbstractField {
    private final ChangeSchema.DataType dataType;

    public Field(ChangeSchema.DataType dataType, Object value) {
        super(value);
        this.dataType = Preconditions.checkNotNull(dataType);
    }

    public ChangeSchema.DataType getDataType() {
        return dataType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Field field = (Field) o;
        return dataType.equals(field.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataType);
    }
}
