package com.scylladb.cdc.model.worker.cql;

import java.util.Objects;

/*
 * Represents CQL type DURATION
 */
public class CqlDuration {
    private final int months;
    private final int days;
    private final long nanoseconds;

    public CqlDuration(int months, int days, long nanoseconds) {
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
        return String.format("Duration(%d, %d, %d)", months, days, nanoseconds);
    }
}
