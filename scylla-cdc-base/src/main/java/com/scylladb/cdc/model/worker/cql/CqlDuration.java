package com.scylladb.cdc.model.worker.cql;

import java.util.Objects;

/*
 * Represents CQL type DURATION
 */
public class CqlDuration {
    private final long months;
    private final long days;
    private final long nanoseconds;

    public CqlDuration(long months, long days, long nanoseconds) {
        this.months = months;
        this.days = days;
        this.nanoseconds = nanoseconds;
    }

    public long getMonths() {
        return months;
    }

    public long getDays() {
        return days;
    }

    public long getNanoseconds() {
        return nanoseconds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CqlDuration cqlDuration = (CqlDuration) o;
        return months == cqlDuration.months &&
                days == cqlDuration.days &&
                nanoseconds == cqlDuration.nanoseconds;
    }

    @Override
    public int hashCode() {
        return Objects.hash(months, days, nanoseconds);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (months < 0 || days < 0 || nanoseconds < 0) {
            sb.append('-');
        }

        long monthRemainder = appendUnit(sb, Math.abs(months), 12, "y");
        appendUnit(sb, monthRemainder, 1, "mo");

        appendUnit(sb, Math.abs(days), 1, "d");

        long nanosecondRemainder = appendUnit(sb, Math.abs(nanoseconds), 3600000000000L, "h");
        nanosecondRemainder = appendUnit(sb, nanosecondRemainder, 60000000000L, "m");
        nanosecondRemainder = appendUnit(sb, nanosecondRemainder, 1000000000, "s");
        nanosecondRemainder = appendUnit(sb, nanosecondRemainder, 1000000, "ms");
        nanosecondRemainder = appendUnit(sb, nanosecondRemainder, 1000, "us");
        appendUnit(sb, nanosecondRemainder, 1, "ns");

        return sb.toString();
    }

    private long appendUnit(StringBuilder sb, long count, long unitFactor, String unitName) {
        if (count == 0 || count < unitFactor) {
            return count;
        }

        sb.append(count / unitFactor).append(unitName);
        return count % unitFactor;
    }
}
