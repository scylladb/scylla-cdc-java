package com.scylladb.cdc.model.worker;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.stream.Stream;

import com.scylladb.cdc.model.worker.cql.Cell;

/*
 * Represents a single CDC log row,
 * without any post-processing.
 */
public interface RawChange extends Iterable<Cell> {
    public enum OperationType {
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

        byte operationId;
        OperationType(byte operationId) {
            this.operationId = operationId;
        }

        public static OperationType parse(byte value) {
            try {
                return OperationType.values()[value];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(Byte.toString(value), e);
            }
        }
    }

    ChangeId getId();

    default OperationType getOperationType() {
        Byte operation = getCell("cdc$operation").getByte();
        return OperationType.parse(operation);
    }

    default boolean isEndOfBatch() {
        Boolean eob = getCell("cdc$end_of_batch").getBoolean();
        return eob != null && eob.booleanValue();
    }

    default int getBatchSequenceNumber() {
        return getCell("cdc$batch_seq_no").getInt();
    }
    
    default Long getTTL() {
        return getCell("cdc$ttl").getLong();
    }

    ChangeSchema getSchema();

    /*
     * Gets the value of column as Java Object.
     */
    default Object getAsObject(String columnName) {
        return getAsObject(getSchema().getColumnDefinition(columnName));
    }
    
    Object getAsObject(ChangeSchema.ColumnDefinition c);

    default Cell getCell(String columnName) {
        return getCell(getSchema().getColumnDefinition(columnName));        
    }

    Cell getCell(ChangeSchema.ColumnDefinition c);

    default boolean isNull(String columnName) {
        return isNull(getSchema().getColumnDefinition(columnName));
    }

    boolean isNull(ChangeSchema.ColumnDefinition c);

    default ByteBuffer getUnsafeBytes(String columnName) {
        return getUnsafeBytes(getSchema().getColumnDefinition(columnName));
    }
    
    ByteBuffer getUnsafeBytes(ChangeSchema.ColumnDefinition c);

    default boolean isDeleted(String columnName) {
        return isDeleted(getSchema().getColumnDefinition(columnName));
    }

    default boolean isDeleted(ChangeSchema.ColumnDefinition c) {
        ChangeSchema.DataType type = c.getBaseTableDataType();
        if (type != null && type.isAtomic()) {
            return isNull(c);
        }
        // if type == null, this will throw
        Boolean value = getCell(c.getDeletedColumn(getSchema())).getBoolean();
        return value != null && value;
    }

    @Deprecated
    default boolean getIsDeleted(String columnName) {
        return isDeleted(columnName);
    }

    @Override
    default Iterator<Cell> iterator() {
        return stream().iterator();
    }
    
    default Iterator<Cell> data() {
        return dataStream().iterator();
    }

    default Iterator<Cell> metadata() {
        return dataStream().iterator();
    }
    
    default Stream<Cell> stream() {
        return getSchema().getAllColumnDefinitions().stream().map(c -> getCell(c));
    }

    default Stream<Cell> dataStream() {
        return getSchema().getNonCdcColumnDefinitions().stream().map(c -> getCell(c));
    }

    default Stream<Cell> metadataStream() {
        return getSchema().getCdcColumnDefinitions().stream().map(c -> getCell(c));
    }

}
