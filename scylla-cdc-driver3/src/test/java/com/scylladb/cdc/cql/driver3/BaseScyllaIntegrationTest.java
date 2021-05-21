package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.model.*;
import com.scylladb.cdc.model.worker.Task;
import com.scylladb.cdc.model.worker.TaskState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BaseScyllaIntegrationTest {
    protected static final long SCYLLA_TIMEOUT_MS = 3000;

    protected Session driverSession;
    protected Cluster driverCluster;

    private String hostname;
    private int port;
    private Driver3Session librarySession;

    @BeforeEach
    public void beforeEach() {
        Properties systemProperties = System.getProperties();

        hostname = Preconditions.checkNotNull(systemProperties.getProperty("scylla.docker.hostname"));
        port = Integer.parseInt(systemProperties.getProperty("scylla.docker.port"));

        driverCluster = Cluster.builder()
                .addContactPointsWithPorts(new InetSocketAddress(hostname, port))
                .build();
        driverSession = driverCluster.connect();

        // Drop the test keyspace in case a prior cleanup was not properly executed.
        driverSession.execute(SchemaBuilder.dropKeyspace("ks").ifExists());

        // Create a test keyspace.
        driverSession.execute(SchemaBuilder.createKeyspace("ks").with().replication(
            new HashMap<String, Object>() {{
                put("class", "SimpleStrategy");
                put("replication_factor", "1");
        }}));
    }

    /**
     * Returns a cached {@link Driver3Session} session or builds it.
     * <p>
     * The session is only built once per
     * each test and is cached between different invocations
     * of this method. The cached session is closed and
     * removed after each test (in {@link #afterEach()}).
     *
     * @return a {@link Driver3Session} session for a test. It is cached
     *         between this method's invocations.
     */
    public Driver3Session buildLibrarySession() {
        if (librarySession == null) {
            CQLConfiguration cqlConfiguration = CQLConfiguration.builder()
                    .addContactPoint(hostname, port)
                    .build();
            librarySession = new Driver3Session(cqlConfiguration);
        }

        return librarySession;
    }

    @AfterEach
    public void afterEach() {
        if (driverSession != null) {
            // Drop the test keyspace.
            driverSession.execute(SchemaBuilder.dropKeyspace("ks").ifExists());
            driverSession.close();
            driverSession = null;
        }
        if (driverCluster != null) {
            driverCluster.close();
            driverCluster = null;
        }
        if (librarySession != null) {
            librarySession.close();
            librarySession = null;
        }
    }
}
