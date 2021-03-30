package com.scylladb.cdc.cql.driver3;

import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.cql.Field;

// Public only for cql-replicator. It has it's own copy of the translators, 
// and need to create this type. This is not good, but roll with it for now.
// I am however calling bs on the "shading" argument, since why would you
// shade repl and driver3 separately?
public class Driver3Field implements Field {
    private final Object value;
    private final ChangeSchema.DataType dataType;

    public Driver3Field(ChangeSchema.DataType dataType, Object value) {
        this.value = value;
        this.dataType = Preconditions.checkNotNull(dataType);
    }

    @Override
    public ChangeSchema.DataType getDataType() {
        return dataType;
    }

    @Override
    public Object getAsObject() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataType, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Driver3Field)) {
            return false;
        }
        Driver3Field other = (Driver3Field) obj;
        return Objects.equals(dataType, other.dataType) && Objects.equals(value, other.value);
    }

    @Override
    public String toString() {
        if (value != null && value instanceof ByteBuffer) {
            byte[] valueArray = ((ByteBuffer) value).array();
            return BaseEncoding.base16().encode(valueArray, 0, valueArray.length);
        }
        return Objects.toString(value);
    }
}
