package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.CQLConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * Runs a test against a keyspace {@code ks} that uses tablets.
 * <p>
 * The test is skipped on a ScyllaDB version without tablets or without CDC on tablets. On
 * {@code latest} that would be a regression, so the test fails instead.
 */
@Tag("integration")
@Tag("tablets")
public class BaseScyllaTabletsIntegrationTest {
    protected CqlSession driverSession;

    private String hostname;
    private int port;
    private String scyllaVersion;
    private Driver4Session librarySession;

    @BeforeEach
    public void beforeEach() {
        hostname = Preconditions.checkNotNull(System.getProperty("scylla.docker.hostname"));
        port = Integer.parseInt(System.getProperty("scylla.docker.port"));
        scyllaVersion = Preconditions.checkNotNull(System.getProperty("scylla.docker.version"));

        driverSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(hostname, port))
                .build();

        // Drop the test keyspace in case a prior cleanup was not properly executed.
        driverSession.execute("DROP KEYSPACE IF EXISTS ks");

        try {
            driverSession.execute("CREATE KEYSPACE ks WITH replication = "
                    + "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                    + "AND tablets = {'initial': 2}");
        } catch (QueryValidationException e) {
            if (e.getMessage() != null && e.getMessage().contains("Unknown property 'tablets'")) {
                failOnLatestElseAbort("This ScyllaDB version does not support tablets: " + e.getMessage());
            }
            throw e;
        }

        try {
            driverSession.execute("CREATE TABLE ks.cdc_probe (pk int PRIMARY KEY) WITH cdc = {'enabled': true}");
            driverSession.execute("DROP TABLE ks.cdc_probe");
        } catch (QueryValidationException e) {
            if (e.getMessage() != null && e.getMessage().contains("Cannot create CDC log for a table")
                    && e.getMessage().contains("keyspace uses tablets")) {
                failOnLatestElseAbort("This ScyllaDB version does not support CDC with tablets: " + e.getMessage());
            }
            throw e;
        }
    }

    /**
     * Aborts the test, so it is reported as skipped, or fails it on {@code latest}, where every
     * capability probed in {@link #beforeEach()} must be present.
     */
    private void failOnLatestElseAbort(String reason) {
        if ("latest".equals(scyllaVersion)) {
            fail("ScyllaDB latest must support CDC with tablets. " + reason);
        }
        abort(reason);
    }

    /**
     * Returns the {@link Driver4Session} of this test, building it on the first call.
     */
    protected Driver4Session buildLibrarySession() {
        if (librarySession == null) {
            librarySession = new Driver4Session(CQLConfiguration.builder()
                    .addContactPoint(hostname, port)
                    .build());
        }
        return librarySession;
    }

    @AfterEach
    public void afterEach() {
        try {
            if (librarySession != null) {
                librarySession.close();
            }
        } finally {
            librarySession = null;
            if (driverSession != null) {
                try {
                    driverSession.execute("DROP KEYSPACE IF EXISTS ks");
                } finally {
                    driverSession.close();
                    driverSession = null;
                }
            }
        }
    }
}
