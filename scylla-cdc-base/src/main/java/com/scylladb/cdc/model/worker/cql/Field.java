package com.scylladb.cdc.model.worker.cql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.scylladb.cdc.model.worker.ChangeSchema;

public interface Field {
    public Object getAsObject();

    public ChangeSchema.DataType getDataType();

    default boolean isNull() {
        return getAsObject() == null;
    }
    
    default ByteBuffer getBytes() {
        return (ByteBuffer) getAsObject();
    }

    default String getString() {
        return (String) getAsObject();
    }

    default BigInteger getVarint() {
        return (BigInteger) getAsObject();
    }

    default BigDecimal getDecimal() {
        return (BigDecimal) getAsObject();
    }

    default UUID getUUID() {
        return (UUID) getAsObject();
    }

    default InetAddress getInet() {
        return (InetAddress) getAsObject();
    }

    default Float getFloat() {
        return (Float) getAsObject();
    }

    default Double getDouble() {
        return (Double) getAsObject();
    }

    default Long getLong() {
        return (Long) getAsObject();
    }

    default Integer getInt() {
        return (Integer) getAsObject();
    }

    default Short getShort() {
        return (Short) getAsObject();
    }

    default Byte getByte() {
        return (Byte) getAsObject();
    }

    default Boolean getBoolean() {
        return (Boolean) getAsObject();
    }

    @SuppressWarnings("unchecked")
    default Map<Field, Field> getMap() {
        return (Map<Field, Field>) getAsObject();
    }

    @SuppressWarnings("unchecked")
    default Set<Field> getSet() {
        return (Set<Field>) getAsObject();
    }

    @SuppressWarnings("unchecked")
    default List<Field> getList() {
        return (List<Field>) getAsObject();
    }

    @SuppressWarnings("unchecked")
    default Map<String, Field> getUDT() {
        return (Map<String, Field>) getAsObject();
    }

    @SuppressWarnings("unchecked")
    default List<Field> getTuple() {
        return (List<Field>) getAsObject();
    }

    default Date getTimestamp() {
        return (Date) getAsObject();
    }

    default Long getTime() {
        return (Long) getAsObject();
    }

    default CqlDuration getDuration() {
        return (CqlDuration) getAsObject();
    }

    default CqlDate getDate() {
        return (CqlDate) getAsObject();
    }
}
