package com.scylladb.cdc.model.worker;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ChangeTimeTest {
    private static final UUID BASE_UUID = TimeUUID.middleOf(300_000);

    private static ChangeTime withLeastSignificantBits(long bits) {
        return new ChangeTime(new UUID(BASE_UUID.getMostSignificantBits(), bits));
    }

    @Test
    public void comparesDistinctUuidsWithEqualTimestamps() {
        ChangeTime first = withLeastSignificantBits(0);
        ChangeTime second = withLeastSignificantBits(1);

        assertEquals(first.getUUID().timestamp(), second.getUUID().timestamp());
        assertTrue(first.compareTo(second) < 0);
        assertTrue(second.compareTo(first) > 0);
    }

    @Test
    public void comparesEveryLeastSignificantByteAsSigned() {
        for (int shift = 0; shift < Long.SIZE; shift += Byte.SIZE) {
            // Keep higher bytes equal so the byte at shift decides the ordering.
            long precedingBytes = shift == 56 ? 0 : -1L << (shift + Byte.SIZE);
            ChangeTime negative = withLeastSignificantBits(precedingBytes | (0x80L << shift));
            ChangeTime positive = withLeastSignificantBits(precedingBytes | (0x7fL << shift));

            assertTrue(negative.compareTo(positive) < 0, "byte shift " + shift);
            assertTrue(positive.compareTo(negative) > 0, "byte shift " + shift);
        }
    }

    @Test
    public void comparesLeastSignificantBytesInNetworkOrder() {
        ChangeTime first = withLeastSignificantBits(0x0100000000000080L);
        ChangeTime second = withLeastSignificantBits(0x000000000000007fL);

        assertTrue(first.compareTo(second) > 0);
        assertTrue(second.compareTo(first) < 0);
    }

    @Test
    public void equalityAndAntisymmetry() {
        ChangeTime first = withLeastSignificantBits(0x80ff000000000001L);
        ChangeTime same = withLeastSignificantBits(0x80ff000000000001L);
        ChangeTime other = withLeastSignificantBits(0x80ff000000000002L);

        assertEquals(0, first.compareTo(same));
        assertEquals(first, same);
        assertTrue(first.compareTo(other) < 0);
        assertEquals(-Integer.signum(other.compareTo(first)), Integer.signum(first.compareTo(other)));
    }

    @Test
    public void timestampTakesPrecedenceOverLeastSignificantBytes() {
        ChangeTime earlier = new ChangeTime(new UUID(
                TimeUUID.middleOf(300_000).getMostSignificantBits(), 0x7f00000000000000L));
        ChangeTime later = new ChangeTime(new UUID(
                TimeUUID.middleOf(300_001).getMostSignificantBits(), 0x8000000000000000L));

        assertTrue(earlier.compareTo(later) < 0);
        assertTrue(later.compareTo(earlier) > 0);
    }
}
