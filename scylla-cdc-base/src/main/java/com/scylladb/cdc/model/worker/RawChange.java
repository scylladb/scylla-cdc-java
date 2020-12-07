package com.scylladb.cdc.model.worker;

import com.datastax.driver.core.Row;
import com.sun.org.apache.xpath.internal.operations.Bool;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;

/*
 * Represents a single CDC log row,
 * without any post-processing.
 */
public interface RawChange {
    enum OperationType {
        PRE_IMAGE((byte) 0),
        ROW_UPDATE((byte) 1),
        ROW_INSERT((byte) 2),
        ROW_DELETE((byte) 3),
        PARTITION_DELETE((byte) 4),
        ROW_RANGE_DELETE_INCLUSIVE_LEFT_BOUND((byte) 5),
        ROW_RANGE_DELETE_EXCLUSIVE_LEFT_BOUND((byte) 6),
        ROW_RANGE_DELETE_INCLUSIVE_RIGHT_BOUND((byte) 7),
        ROW_RANGE_DELETE_EXCLUSIVE_RIGHT_BOUND((byte) 8),
        POST_IMAGE((byte) 9);

        // Assumes that operationId are consecutive and start from 0.
        private static final OperationType[] enumValues = OperationType.values();

        byte operationId;
        OperationType(byte operationId) {
            this.operationId = operationId;
        }

        public static OperationType parse(byte value) {
            // TODO - validation
            return enumValues[value];
        }
    }

    ChangeId getId();

    default OperationType getOperationType() {
        return OperationType.parse(getByte(quoteIfNecessary("cdc$operation")));
    }

    default Long getTTL() {
        return getLong(quoteIfNecessary("cdc$ttl"));
    }

    ChangeSchema getSchema();

    /*
     * Gets the value of column as Java Object.
     */
    Object getAsObject(String columnName);

    /*
     * Gets the value of column as ByteBuffer.
     * (As it was returned by a driver)
     */
    ByteBuffer getAsBytes(String columnName);

    default ByteBuffer getBytes(String columnName) {
        return (ByteBuffer) getAsObject(columnName);
    }

    default String getString(String columnName) {
        return (String) getAsObject(columnName);
    }

    default BigInteger getVarint(String columnName) {
        return (BigInteger) getAsObject(columnName);
    }

    default BigDecimal getDecimal(String columnName) {
        return (BigDecimal) getAsObject(columnName);
    }

    default UUID getUUID(String columnName) {
        return (UUID) getAsObject(columnName);
    }

    default InetAddress getInet(String columnName) {
        return (InetAddress) getAsObject(columnName);
    }

    default Float getFloat(String columnName) {
        return (Float) getAsObject(columnName);
    }

    default Double getDouble(String columnName) {
        return (Double) getAsObject(columnName);
    }

    default Long getLong(String columnName) {
        return (Long) getAsObject(columnName);
    }

    default Integer getInt(String columnName) {
        return (Integer) getAsObject(columnName);
    }

    default Short getShort(String columnName) {
        return (Short) getAsObject(columnName);
    }

    default Byte getByte(String columnName) {
        return (Byte) getAsObject(columnName);
    }

    default Boolean getBoolean(String columnName) {
        return (Boolean) getAsObject(columnName);
    }

    default Map getMap(String columnName) {
        return (Map) getAsObject(columnName);
    }

    default Set getSet(String columnName) {
        return (Set) getAsObject(columnName);
    }

    default List getList(String columnName) {
        return (List) getAsObject(columnName);
    }

    /*
     * What follows are temporary methods
     * used for porting the replicator
     * from old library to new library.
     *
     * Those methods should be removed
     * after the porting process is done.
     */

    @Deprecated
    boolean TEMPORARY_PORTING_isDeleted(String name);

    @Deprecated
    Row TEMPORARY_PORTING_row();
}
