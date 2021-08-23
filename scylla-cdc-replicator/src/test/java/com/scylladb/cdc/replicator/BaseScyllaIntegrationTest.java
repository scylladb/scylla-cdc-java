package com.scylladb.cdc.replicator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.cql.driver3.Driver3MasterCQL;
import com.scylladb.cdc.cql.driver3.Driver3Session;
import com.scylladb.cdc.model.*;
import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BaseScyllaIntegrationTest {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    protected static final long SCYLLA_TIMEOUT_MS = 3000;

    protected Session driverSessionSrc;
    protected Session driverSessionDst;
    protected Cluster driverClusterSrc;
    protected Cluster driverClusterDst;

    protected String hostnameSrc;
    protected String hostnameDst;
    protected int portSrc;
    protected int portDst;

    private Session librarySessionSrc;
    private Session librarySessionDst;

    private String scyllaVersion;
    private boolean hasWaitedForFirstGeneration = false;

    @BeforeAll
    public void beforeAll() throws ExecutionException, InterruptedException, TimeoutException {
        Properties systemProperties = System.getProperties();

        hostnameSrc = Preconditions.checkNotNull(systemProperties.getProperty("scylla_src.docker.hostname"));
        hostnameDst = Preconditions.checkNotNull(systemProperties.getProperty("scylla_dst.docker.hostname"));
        portSrc = Integer.parseInt(systemProperties.getProperty("scylla_src.docker.port"));
        portDst = Integer.parseInt(systemProperties.getProperty("scylla_dst.docker.port"));
        scyllaVersion = Preconditions.checkNotNull(systemProperties.getProperty("scylla.docker.version"));

        driverClusterSrc = Cluster.builder()
                .addContactPointsWithPorts(new InetSocketAddress(hostnameSrc, portSrc))
                .build();
        driverClusterDst = Cluster.builder()
                .addContactPointsWithPorts(new InetSocketAddress(hostnameDst, portDst))
                .build();
        driverSessionSrc = driverClusterSrc.connect();
        driverSessionDst = driverClusterDst.connect();

        // Drop the test keyspace in case a prior cleanup was not properly executed.
        driverSessionSrc.execute(SchemaBuilder.dropKeyspace("ks").ifExists());
        driverSessionDst.execute(SchemaBuilder.dropKeyspace("ks").ifExists());

        // Create a test keyspace.
        driverSessionSrc.execute(SchemaBuilder.createKeyspace("ks").with().replication(
            new HashMap<String, Object>() {{
                put("class", "SimpleStrategy");
                put("replication_factor", "1");
        }}));
        driverSessionDst.execute(SchemaBuilder.createKeyspace("ks").with().replication(
            new HashMap<String, Object>() {{
                put("class", "SimpleStrategy");
                put("replication_factor", "1");
        }}));

        maybeWaitForFirstGeneration();
    }

    public Session buildLibrarySessionSrc() {
        if (librarySessionSrc == null) {
            Cluster clusterSrc = Cluster.builder()
                .addContactPointsWithPorts(new InetSocketAddress(hostnameSrc, portSrc))
                .build();
            librarySessionSrc = clusterSrc.connect();
        }

        return librarySessionSrc;
    }

    public Session buildLibrarySessionDst() {
        if (librarySessionDst == null) {
            Cluster clusterDst = Cluster.builder()
            .addContactPointsWithPorts(new InetSocketAddress(hostnameDst, portDst))
            .build();
            librarySessionDst = clusterDst.connect();
        }

        return librarySessionDst;
    }

    @AfterAll
    public void afterAll() {
        if (driverSessionSrc != null) {
            // Drop the test keyspace.
            driverSessionSrc.execute(SchemaBuilder.dropKeyspace("ks").ifExists());
            driverSessionSrc.close();
            driverSessionSrc = null;
        }
        if (driverSessionDst != null) {
            // Drop the test keyspace.
            driverSessionDst.execute(SchemaBuilder.dropKeyspace("ks").ifExists());
            driverSessionDst.close();
            driverSessionDst = null;
        }
        if (driverClusterSrc != null) {
            driverClusterSrc.close();
            driverClusterSrc = null;
        }
        if (driverClusterDst != null) {
            driverClusterDst.close();
            driverClusterDst = null;
        }
        if (librarySessionSrc != null) {
            librarySessionSrc.close();
            librarySessionSrc = null;
        }
        if (librarySessionDst != null) {
            librarySessionDst.close();
            librarySessionDst = null;
        }
    }

    /**
     * Waits for first CDC generation to start
     * up. This method is only relevant for
     * Scylla 4.3, as starting with Scylla 4.4
     * adding ring delay for the first generation
     * has been removed: <a href="https://github.com/scylladb/scylla/pull/7654">Pull request #7654</a>,
     * in which case this method is a no-op. This
     * method waits for the first generation only
     * once and the next invocations immediately
     * return.
     * <p>
     * After waiting for first generation to start
     * it is safe to <code>INSERT</code> rows
     * into the CDC log table without the
     * worry of <code>could not find any CDC stream</code>
     * error.
     */
    private void maybeWaitForFirstGeneration() throws ExecutionException, InterruptedException, TimeoutException {
        if (!scyllaVersion.startsWith("4.3.")) {
            return;
        }

        if (hasWaitedForFirstGeneration) {
            return;
        }

        CQLConfiguration cqlConfiguration = CQLConfiguration.builder()
                    .addContactPoint(hostnameSrc, portSrc)
                    .build();
        Driver3Session librarySession = new Driver3Session(cqlConfiguration);

        // Get the first generation id.
        MasterCQL masterCQL = new Driver3MasterCQL(librarySession);
        GenerationId generationId = masterCQL.fetchFirstGenerationId()
                .get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS).get();
        
        librarySession.close();

        // Wait for the generation to start.
        Date now = new Date();
        Date generationStart = generationId.getGenerationStart().toDate();
        long millisecondsToWait = generationStart.getTime() - now.getTime();
        if (millisecondsToWait > 0) {
            logger.atInfo().log("Waiting for the first generation for %d ms.", millisecondsToWait);
            Thread.sleep(millisecondsToWait);
        }

        hasWaitedForFirstGeneration = true;
    }
}
