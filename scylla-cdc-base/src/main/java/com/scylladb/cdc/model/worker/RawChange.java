package com.scylladb.cdc.model.worker;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.stream.Stream;

import com.scylladb.cdc.model.StreamId;
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

    default ChangeId getId() {
        return new ChangeId(new StreamId(getCell("cdc$stream_id").getBytes()),
                new ChangeTime(getCell("cdc$time").getUUID()));
    }

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

    /**
     * Returns the value of a binary representation of the specified column as a <code>ByteBuffer</code>.
     * <p>
     * This method returns a binary representation of the specified column as it was received
     * by the underlying Scylla driver. This representation may vary between different
     * versions of this library and <code>WorkerCQL</code> implementations.
     * <p>
     * This method can be called for any type of column, not only <code>BLOB</code>.
     * If you want to read the value of a <code>BLOB</code> column, please use the
     * {@link #getCell(String)} method and call {@link Cell#getBytes()}.
     * <p>
     * If a value of this column is <code>NULL</code>, this method returns <code>null</code>.
     *
     * @param columnName the column name to retrieve.
     * @return the value of a binary representation of the specified column.
     *         If the value of this column is <code>NULL</code>, <code>null</code> is returned.
     */
    default ByteBuffer getAsUnsafeBytes(String columnName) {
        return getAsUnsafeBytes(getSchema().getColumnDefinition(columnName));
    }

    /**
     * Returns the value of a binary representation of the specified column as a <code>ByteBuffer</code>.
     * <p>
     * This method returns a binary representation of the specified column as it was received
     * by the underlying Scylla driver. This representation may vary between different
     * versions of this library and <code>WorkerCQL</code> implementations.
     * <p>
     * This method can be called for any type of column, not only <code>BLOB</code>.
     * If you want to read the value of a <code>BLOB</code> column, please use the
     * {@link #getCell(ChangeSchema.ColumnDefinition)} )} method and call {@link Cell#getBytes()}.
     * <p>
     * If a value of this column is <code>NULL</code>, this method returns <code>null</code>.
     *
     * @param columnDefinition the column to retrieve.
     * @return the value of a binary representation of the specified column.
     *         If the value of this column is <code>NULL</code>, <code>null</code> is returned.
     */
    ByteBuffer getAsUnsafeBytes(ChangeSchema.ColumnDefinition columnDefinition);

    default boolean isDeleted(String columnName) {
        return isDeleted(getSchema().getColumnDefinition(columnName));
    }

    default boolean isDeleted(ChangeSchema.ColumnDefinition c) {
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

    default Iterator<Cell> nonCdcColumnsIterator() {
        return nonCdcColumnsStream().iterator();
    }

    default Iterator<Cell> cdcColumnsIterator() {
        return cdcColumnsStream().iterator();
    }

    default Stream<Cell> stream() {
        return getSchema().getAllColumnDefinitions().stream().map(c -> getCell(c));
    }

    default Stream<Cell> nonCdcColumnsStream() {
        return getSchema().getNonCdcColumnDefinitions().stream().map(c -> getCell(c));
    }

    default Stream<Cell> cdcColumnsStream() {
        return getSchema().getCdcColumnDefinitions().stream().map(c -> getCell(c));
    }

}
