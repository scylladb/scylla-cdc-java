package com.scylladb.cdc.model.worker;

import com.scylladb.cdc.model.cql.Cell;

import java.nio.ByteBuffer;
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

    /*
     * Represents CQL type DURATION
     */
    class Duration {
        // months, days and nanoseconds
        private final int months;
        private final int days;
        private final long nanoseconds;

        public Duration(int months, int days, long nanoseconds) {
            this.months = months;
            this.days = days;
            this.nanoseconds = nanoseconds;
        }

        public int getMonths() {
            return months;
        }

        public int getDays() {
            return days;
        }

        public long getNanoseconds() {
            return nanoseconds;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Duration duration = (Duration) o;
            return months == duration.months &&
                    days == duration.days &&
                    nanoseconds == duration.nanoseconds;
        }

        @Override
        public int hashCode() {
            return Objects.hash(months, days, nanoseconds);
        }

        @Override
        public String toString() {
            return String.format("Duration(%d, %d, %d)", months, days, nanoseconds);
        }
    }

    /*
     * Represents CQL type DATE
     */
    class CqlDate {
        private final int year;
        private final int month;
        private final int day;

        public CqlDate(int year, int month, int day) {
            this.year = year;
            this.month = month;
            this.day = day;
        }

        public int getYear() {
            return year;
        }

        public int getMonth() {
            return month;
        }

        public int getDay() {
            return day;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CqlDate cqlDate = (CqlDate) o;
            return year == cqlDate.year &&
                    month == cqlDate.month &&
                    day == cqlDate.day;
        }

        @Override
        public int hashCode() {
            return Objects.hash(year, month, day);
        }

        private String pad(String s) {
            if (s.length() < 2) {
                return "0" + s;
            }
            return s;
        }

        @Override
        public String toString() {
            return year + "-" + pad(Integer.toString(month)) + "-" + pad(Integer.toString(day));
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
