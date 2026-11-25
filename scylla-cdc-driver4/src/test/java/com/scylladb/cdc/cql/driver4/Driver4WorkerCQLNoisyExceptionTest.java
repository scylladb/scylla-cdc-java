package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.connection.BusyConnectionException;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Map.Entry;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Driver4WorkerCQLNoisyExceptionTest {
    private static final Node HOST_A = node("A");
    private static final Node HOST_B = node("B");

    @Test
    public void testClassifiesDirectTransientFailures() {
        assertTrue(Driver4WorkerCQL.isNoisyDriverFailure(new BusyConnectionException(1)));
        assertTrue(Driver4WorkerCQL.isNoisyDriverFailure(new OverloadedException(HOST_A, "overloaded")));
        assertTrue(Driver4WorkerCQL.isNoisyDriverFailure(
                new ReadTimeoutException(HOST_A, DefaultConsistencyLevel.ONE, 1, 0, false)));
        assertFalse(Driver4WorkerCQL.isNoisyDriverFailure(new IllegalStateException("unexpected")));
    }

    @Test
    public void testAllNodesFailureRequiresEveryAttemptToBeTransient() {
        Throwable busy = new BusyConnectionException(1);
        Throwable overloaded = new OverloadedException(HOST_B, "overloaded");
        assertTrue(Driver4WorkerCQL.isNoisyDriverFailure(failure(entry(HOST_A, busy), entry(HOST_B, overloaded))));
        assertFalse(Driver4WorkerCQL.isNoisyDriverFailure(
                failure(entry(HOST_A, busy), entry(HOST_B, new IllegalStateException("unexpected")))));
        // A node retried after a transient error, then failed for another reason.
        assertFalse(Driver4WorkerCQL.isNoisyDriverFailure(
                failure(entry(HOST_A, busy), entry(HOST_A, new IllegalStateException("unexpected")))));
        assertFalse(Driver4WorkerCQL.isNoisyDriverFailure(new NoNodeAvailableException()));
    }

    @SafeVarargs
    private static AllNodesFailedException failure(Entry<Node, Throwable>... errors) {
        return AllNodesFailedException.fromErrors(Arrays.asList(errors));
    }

    private static Entry<Node, Throwable> entry(Node node, Throwable error) {
        return new SimpleEntry<>(node, error);
    }

    private static Node node(String name) {
        return (Node) Proxy.newProxyInstance(Node.class.getClassLoader(), new Class<?>[] {Node.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "equals":
                            return proxy == args[0];
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "toString":
                            return name;
                        default:
                            throw new UnsupportedOperationException(method.getName());
                    }
                });
    }
}
