package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.exceptions.BusyPoolException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Driver3WorkerCQLNoisyExceptionTest {
    private static final EndPoint HOST_A = () -> new InetSocketAddress("127.0.0.1", 9042);
    private static final EndPoint HOST_B = () -> new InetSocketAddress("127.0.0.2", 9042);

    @Test
    void classifiesDirectTransientFailures() {
        assertTrue(Driver3WorkerCQL.isNoisyDriverFailure(new BusyPoolException(HOST_A, 1)));
        assertTrue(Driver3WorkerCQL.isNoisyDriverFailure(new OverloadedException(HOST_A, "overloaded")));
        assertTrue(Driver3WorkerCQL.isNoisyDriverFailure(
                new ReadTimeoutException(ConsistencyLevel.ONE, 1, 0, false)));
        assertFalse(Driver3WorkerCQL.isNoisyDriverFailure(new IllegalStateException("unexpected")));
    }

    @Test
    void noHostFailureRequiresAllHostsToHaveTransientErrors() {
        Throwable busy = new BusyPoolException(HOST_A, 1);
        Throwable overloaded = new OverloadedException(HOST_B, "overloaded");

        assertTrue(Driver3WorkerCQL.isNoisyDriverFailure(
                new NoHostAvailableException(Map.of(HOST_A, busy, HOST_B, overloaded))));
        assertFalse(Driver3WorkerCQL.isNoisyDriverFailure(
                new NoHostAvailableException(Map.of(HOST_A, busy, HOST_B, new IllegalStateException("unexpected")))));
        assertFalse(Driver3WorkerCQL.isNoisyDriverFailure(
                new NoHostAvailableException(Collections.emptyMap())));
    }
}
