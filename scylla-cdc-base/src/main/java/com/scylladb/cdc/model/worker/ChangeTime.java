package com.scylladb.cdc.model.worker;

import com.google.common.base.Preconditions;

import java.util.Date;
import java.util.Objects;
import java.util.UUID;

public class ChangeTime implements Comparable<ChangeTime> {
    private final UUID time;

    public ChangeTime(UUID time) {
        this.time = Preconditions.checkNotNull(time);
    }

    public UUID getUUID() {
        return time;
    }

    public long getTimestamp() {
        return (time.timestamp() - 0x01b21dd213814000L) / 10;
    }

    public Date getDate() {
        return new Date(getTimestamp() / 1000);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChangeTime that = (ChangeTime) o;
        return time.equals(that.time);
    }

    @Override
    public int hashCode() {
        return Objects.hash(time);
    }

    @Override
    public int compareTo(ChangeTime changeTime) {
        return time.compareTo(changeTime.time);
    }

    @Override
    public String toString() {
        return String.format("ChangeTime(%s)", time);
    }
}
