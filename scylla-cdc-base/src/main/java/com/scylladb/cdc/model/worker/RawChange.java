package com.scylladb.cdc.model.worker;

import com.scylladb.cdc.model.worker.cql.Cell;

import java.util.Objects;

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
        Byte operation = getCell("cdc$operation").getByte();
        return OperationType.parse(operation);
    }

    default Long getTTL() {
        return getCell("cdc$ttl").getLong();
    }

    ChangeSchema getSchema();

    /*
     * Gets the value of column as Java Object.
     */
    Object getAsObject(String columnName);

    default Cell getCell(String columnName) {
        ChangeSchema.ColumnDefinition columnDefinition = getSchema().getColumnDefinition(columnName);
        return new Cell(columnDefinition, getAsObject(columnName));
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
}
