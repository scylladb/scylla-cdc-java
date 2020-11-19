package com.scylladb.cdc.model;

import java.text.DateFormat;
import java.time.temporal.TemporalUnit;
import java.util.Date;

import com.google.common.base.Preconditions;

public final class Timestamp implements Comparable<Timestamp> {
    private final Date value;

    public Timestamp(Date value) {
        this.value = Preconditions.checkNotNull(value);
    }

    public Date toDate() {
        return value;
    }

    public Timestamp plus(long amount, TemporalUnit unit) {
        return new Timestamp(Date.from(toDate().toInstant().plus(amount, unit)));
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Timestamp && value.equals(((Timestamp) o).value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return String.format("Timestamp(%s)", DateFormat.getInstance().format(value));
    }

    @Override
    public int compareTo(Timestamp o) {
        return value.compareTo(o.value);
    }
}
