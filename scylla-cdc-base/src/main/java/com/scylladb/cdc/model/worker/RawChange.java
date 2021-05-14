package com.scylladb.cdc.model.worker;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.model.worker.cql.Field;

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
     * {@link #getCell(ChangeSchema.ColumnDefinition)} method and call {@link Cell#getBytes()}.
     * <p>
     * If a value of this column is <code>NULL</code>, this method returns <code>null</code>.
     *
     * @param columnDefinition the column to retrieve.
     * @return the value of a binary representation of the specified column.
     *         If the value of this column is <code>NULL</code>, <code>null</code> is returned.
     */
    ByteBuffer getAsUnsafeBytes(ChangeSchema.ColumnDefinition columnDefinition);

    /**
     * Returns the boolean value of the deleted column for the given column name.
     * <p>
     * This method returns the value of the <code>cdc$deleted_</code> column for
     * the given column name. It is only relevant for the regular columns
     * (columns not in a primary key) of the base table, because only
     * those columns have a corresponding deleted column.
     * <p>
     * If a column in a delta row is <code>NULL</code> and this method returns <code>true</code> for this
     * column, it means that a <code>NULL</code> value was written in this CDC
     * operation.
     * <p>
     * The meaning of the deleted column is different for preimage rows
     * and mutations of non-frozen collections. See <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-advanced-types/">Advanced column types</a>
     * and <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-preimages/">Preimages and postimages</a> pages for more details.
     *
     * @see <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-basic-operations/">Basic operations in CDC</a>
     * @see <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-advanced-types/">Advanced column types</a>
     * @see <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-preimages/">Preimages and postimages</a>
     * @param columnName the column name for which to retrieve the deleted column.
     * @return the boolean value of the deleted column for the given column.
     */
    default boolean isDeleted(String columnName) {
        return isDeleted(getSchema().getColumnDefinition(columnName));
    }

    /**
     * Returns the boolean value of the deleted column for the given column.
     * <p>
     * This method returns the value of the <code>cdc$deleted_</code> column for
     * the given column. It is only relevant for the regular columns
     * (columns not in a primary key) of the base table, because only
     * those columns have a corresponding deleted column.
     * <p>
     * If a column in a delta row is <code>NULL</code> and this method returns <code>true</code> for this
     * column, it means that a <code>NULL</code> value was written in this CDC
     * operation.
     * <p>
     * The meaning of the deleted column is different for preimage rows
     * and mutations of non-frozen collections. See <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-advanced-types/">Advanced column types</a>
     * and <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-preimages/">Preimages and postimages</a> pages for more details.
     *
     * @see <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-basic-operations/">Basic operations in CDC</a>
     * @see <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-advanced-types/">Advanced column types</a>
     * @see <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-preimages/">Preimages and postimages</a>
     * @param columnDefinition the column for which to retrieve the deleted column.
     * @return the boolean value of the deleted column for the given column.
     */
    default boolean isDeleted(ChangeSchema.ColumnDefinition columnDefinition) {
        ChangeSchema.ColumnDefinition deletedColumnDefinition =
                getSchema().getDeletedColumnDefinition(columnDefinition);
        Boolean value = getCell(deletedColumnDefinition).getBoolean();
        return value != null && value;
    }

    /**
     * Returns the value of the deleted elements column for the given column name.
     * <p>
     * This method returns the value of the <code>cdc$deleted_elements_</code> column
     * for the given column name. It is only relevant for the regular columns
     * (columns not in a primary key) of the base table with non-frozen collection
     * type (for example {@code SET<INT>}).
     * <p>
     * The value of the deleted elements column is a set of {@link Field} that
     * represents which elements were removed from the collection. See
     * <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-advanced-types/">Advanced column types</a>
     * for more details how to interpret the returned set for different data types.
     * <p>
     * Because Scylla does not distinguish a missing value or a <code>NULL</code>
     * value from an empty set, this method returns an empty set instead of returning
     * <code>null</code>.
     *
     * @see <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-advanced-types/">Advanced column types</a>
     * @param columnName the column name for which to retrieve the deleted elements column.
     * @return the value of the deleted elements column as a set of {@link Field}. If the
     *         deleted elements column is <code>NULL</code>, this method returns an empty
     *         set.
     */
    default Set<Field> getDeletedElements(String columnName) {
        return getDeletedElements(getSchema().getColumnDefinition(columnName));
    }

    /**
     * Returns the value of the deleted elements column for the given column.
     * <p>
     * This method returns the value of the <code>cdc$deleted_elements_</code> column
     * for the given column. It is only relevant for the regular columns
     * (columns not in a primary key) of the base table with non-frozen collection
     * type (for example {@code SET<INT>}).
     * <p>
     * The value of the deleted elements column is a set of {@link Field} that
     * represents which elements were removed from the collection. See
     * <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-advanced-types/">Advanced column types</a>
     * for more details how to interpret the returned set for different data types.
     * <p>
     * Because Scylla does not distinguish a missing value or a <code>NULL</code>
     * value from an empty set, this method returns an empty set instead of returning
     * <code>null</code>.
     *
     * @see <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-advanced-types/">Advanced column types</a>
     * @param columnDefinition the column for which to retrieve the deleted elements column.
     * @return the value of the deleted elements column as a set of {@link Field}. If the
     *         deleted elements column is <code>NULL</code>, this method returns an empty
     *         set.
     */
    default Set<Field> getDeletedElements(ChangeSchema.ColumnDefinition columnDefinition) {
        ChangeSchema.ColumnDefinition deletedElementsColumnDefinition =
                getSchema().getDeletedElementsColumnDefinition(columnDefinition);
        return getCell(deletedElementsColumnDefinition).getSet();
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
