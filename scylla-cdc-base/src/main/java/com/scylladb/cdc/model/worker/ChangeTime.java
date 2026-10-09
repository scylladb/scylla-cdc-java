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

    /**
     * Orders CDC cursors like CQL timeuuid values. A timestamp alone is not a unique cursor:
     * two changes can share it, so resume must also compare the remaining UUID bytes.
     */
    @Override
    public int compareTo(ChangeTime changeTime) {
        int timestampComparison = Long.compare(time.timestamp(), changeTime.time.timestamp());
        if (timestampComparison != 0) {
            return timestampComparison;
        }

        long leastSignificantBits = time.getLeastSignificantBits();
        long otherLeastSignificantBits = changeTime.time.getLeastSignificantBits();
        // Flip each byte's sign bit so unsigned long order matches CQL's signed-byte network order.
        return Long.compareUnsigned(leastSignificantBits ^ 0x8080808080808080L,
                otherLeastSignificantBits ^ 0x8080808080808080L);
    }

    @Override
    public String toString() {
        return String.format("ChangeTime(%s)", time);
    }
}
