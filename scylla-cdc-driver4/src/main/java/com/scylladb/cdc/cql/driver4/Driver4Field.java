package com.scylladb.cdc.cql.driver4;

import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.cql.Field;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A library value with its CQL type, the counterpart of {@code Driver3Field}.
 */
final class Driver4Field implements Field {
    private final Object value;
    private final ChangeSchema.DataType dataType;

    Driver4Field(ChangeSchema.DataType dataType, Object value) {
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
        if (!(obj instanceof Driver4Field)) {
            return false;
        }
        Driver4Field other = (Driver4Field) obj;
        return Objects.equals(dataType, other.dataType) && Objects.equals(value, other.value);
    }

    @Override
    public String toString() {
        if (value instanceof ByteBuffer) {
            // Only the readable bytes: 3.x printed the whole backing array.
            ByteBuffer bytes = ((ByteBuffer) value).duplicate();
            byte[] array = new byte[bytes.remaining()];
            bytes.get(array);
            return BaseEncoding.base16().encode(array);
        }
        return Objects.toString(value);
    }
}
