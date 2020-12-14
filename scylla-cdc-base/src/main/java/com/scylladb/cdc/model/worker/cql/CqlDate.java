package com.scylladb.cdc.model.worker.cql;

import java.util.Objects;

/*
 * Represents CQL type DATE
 */
public class CqlDate {
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