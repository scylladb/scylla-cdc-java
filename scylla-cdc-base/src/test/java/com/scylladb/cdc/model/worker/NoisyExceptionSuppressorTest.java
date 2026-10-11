package com.scylladb.cdc.model.worker;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NoisyExceptionSuppressorTest {
    private static final String KEY_A = "table_a";
    private static final String KEY_B = "table_b";

    private static LongSupplier fixedTicker(long millis) {
        long nanos = TimeUnit.MILLISECONDS.toNanos(millis);
        return () -> nanos;
    }

    private static final class MutableTicker implements LongSupplier {
        private final AtomicLong nanos;

        MutableTicker(long initialNanos) {
            nanos = new AtomicLong(initialNanos);
        }

        void advance(long millis) {
            nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
        }

        @Override
        public long getAsLong() {
            return nanos.get();
        }
    }

    @Test
    void disabledWhenWindowIsZero() {
        NoisyExceptionSuppressor suppressor = new NoisyExceptionSuppressor(0, fixedTicker(1000));
        assertFalse(suppressor.shouldSuppress(KEY_A));
        assertFalse(suppressor.shouldSuppress(KEY_A));
    }

    @Test
    void firstCallIsNotSuppressed() {
        NoisyExceptionSuppressor suppressor = new NoisyExceptionSuppressor(60_000, fixedTicker(1000));
        assertFalse(suppressor.shouldSuppress(KEY_A));
    }

    @Test
    void subsequentCallsAreSuppressedWithinWindow() {
        NoisyExceptionSuppressor suppressor = new NoisyExceptionSuppressor(60_000, fixedTicker(1000));
        assertFalse(suppressor.shouldSuppress(KEY_A));
        assertTrue(suppressor.shouldSuppress(KEY_A));
        assertTrue(suppressor.shouldSuppress(KEY_A));
    }

    @Test
    void windowExpiryAllowsLoggingAgain() {
        MutableTicker ticker = new MutableTicker(TimeUnit.MILLISECONDS.toNanos(1000));
        NoisyExceptionSuppressor suppressor = new NoisyExceptionSuppressor(500, ticker);

        assertFalse(suppressor.shouldSuppress(KEY_A));
        assertTrue(suppressor.shouldSuppress(KEY_A));

        ticker.advance(501);

        assertFalse(suppressor.shouldSuppress(KEY_A));
        assertTrue(suppressor.shouldSuppress(KEY_A));
    }

    @Test
    void nanoTimeWrapDoesNotExtendWindow() {
        MutableTicker ticker = new MutableTicker(Long.MAX_VALUE - TimeUnit.MILLISECONDS.toNanos(250));
        NoisyExceptionSuppressor suppressor = new NoisyExceptionSuppressor(500, ticker);

        assertFalse(suppressor.shouldSuppress(KEY_A));
        ticker.advance(501);
        assertFalse(suppressor.shouldSuppress(KEY_A));
    }

    @Test
    void differentKeysAreIndependent() {
        NoisyExceptionSuppressor suppressor = new NoisyExceptionSuppressor(60_000, fixedTicker(1000));

        assertFalse(suppressor.shouldSuppress(KEY_A));
        assertTrue(suppressor.shouldSuppress(KEY_A));
        assertFalse(suppressor.shouldSuppress(KEY_B));
        assertTrue(suppressor.shouldSuppress(KEY_B));
    }

    @Test
    void suppressionCounterReportsPerKey() {
        MutableTicker ticker = new MutableTicker(TimeUnit.MILLISECONDS.toNanos(1000));
        NoisyExceptionSuppressor suppressor = new NoisyExceptionSuppressor(500, ticker);
        Logger julLogger = Logger.getLogger(NoisyExceptionSuppressor.class.getName());
        Level previousLevel = julLogger.getLevel();
        List<LogRecord> records = new ArrayList<>();
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                records.add(record);
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
        };
        julLogger.setLevel(Level.ALL);
        julLogger.addHandler(handler);
        try {
            assertFalse(suppressor.shouldSuppress(KEY_A));
            assertTrue(suppressor.shouldSuppress(KEY_A));
            assertTrue(suppressor.shouldSuppress(KEY_A));
            assertFalse(suppressor.shouldSuppress(KEY_B));

            ticker.advance(501);
            assertFalse(suppressor.shouldSuppress(KEY_A));

            assertEquals(1, records.size());
            assertTrue(records.get(0).getMessage().contains("2 noisy exception(s)"));
            assertTrue(records.get(0).getMessage().contains(KEY_A));
        } finally {
            julLogger.removeHandler(handler);
            julLogger.setLevel(previousLevel);
        }
    }
}
