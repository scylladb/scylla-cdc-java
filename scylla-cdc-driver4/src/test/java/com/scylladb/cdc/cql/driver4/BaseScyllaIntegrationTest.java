package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.CQLConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

import java.net.InetSocketAddress;

/**
 * Runs a test against a keyspace {@code ks} that uses vnodes.
 */
@Tag("integration")
public class BaseScyllaIntegrationTest {
    protected CqlSession driverSession;

    private String hostname;
    private int port;
    private Driver4Session librarySession;

    @BeforeEach
    public void beforeEach() {
        hostname = Preconditions.checkNotNull(System.getProperty("scylla.docker.hostname"));
        port = Integer.parseInt(System.getProperty("scylla.docker.port"));

        driverSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(hostname, port))
                .build();

        // Drop the test keyspace in case a prior cleanup was not properly executed.
        driverSession.execute("DROP KEYSPACE IF EXISTS ks");

        // ScyllaDB 6.x enables tablets by default for NTS. On ScyllaDB < 6.0 the 'tablets'
        // property is unknown; fall back without it.
        try {
            driverSession.execute("CREATE KEYSPACE ks WITH replication = "
                    + "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                    + "AND tablets = {'enabled': false}");
        } catch (QueryValidationException e) {
            if (e.getMessage() == null || !e.getMessage().contains("Unknown property 'tablets'")) {
                throw e;
            }
            driverSession.execute("CREATE KEYSPACE ks WITH replication = "
                    + "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}");
        }
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
