package com.scylladb.cdc.model.worker;

import com.datastax.driver.core.Row;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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

    OperationType getOperationType();

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

    Integer getInt(String columnName);

    Byte getByte(String columnName);

    Boolean getBoolean(String columnName);

    Map getMap(String columnName);

    Set getSet(String columnName);

    /*
     * What follows are temporary methods
     * used for porting the replicator
     * from old library to new library.
     *
     * Those methods should be removed
     * after the porting process is done.
     */

    @Deprecated
    UUID TEMPORARY_PORTING_getTime();

    @Deprecated
    Integer TEMPORARY_PORTING_getTTL();

    @Deprecated
    boolean TEMPORARY_PORTING_isDeleted(String name);

    @Deprecated
    Row TEMPORARY_PORTING_row();
}
