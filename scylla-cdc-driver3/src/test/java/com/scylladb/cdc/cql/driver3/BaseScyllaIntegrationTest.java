package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.*;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
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
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    protected static final long SCYLLA_TIMEOUT_MS = 3000;

    protected Session driverSession;
    protected Cluster driverCluster;

    private String hostname;
    private int port;
    private String scyllaVersion;
    private Driver3Session librarySession;

    private boolean hasWaitedForFirstGeneration = false;

    @BeforeEach
    public void beforeEach() throws ExecutionException, InterruptedException, TimeoutException {
        Properties systemProperties = System.getProperties();

        hostname = Preconditions.checkNotNull(systemProperties.getProperty("scylla.docker.hostname"));
        port = Integer.parseInt(systemProperties.getProperty("scylla.docker.port"));
        scyllaVersion = Preconditions.checkNotNull(systemProperties.getProperty("scylla.docker.version"));

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

        maybeWaitForFirstGeneration();
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

        // Get the first generation id.
        MasterCQL masterCQL = new Driver3MasterCQL(buildLibrarySession());
        GenerationId generationId = masterCQL.fetchFirstGenerationId()
                .get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS).get();

        // Remove the created librarySession,
        // so that it does not interfere
        // with the test code.
        librarySession.close();
        librarySession = null;

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

    /**
     * Creates a {@link Task} which contains the first row in the CDC log.
     * <p>
     * The created {@link Task} queries a single stream id
     * and spans from the beginning of epoch time to the current time. The
     * selected stream id is taken from the first row in the CDC log.
     * <p>
     * A common scenario is to insert a single row to a base table
     * (or multiple within a single partition) and then use this method
     * to build a {@link Task}, which will allow you to read all those
     * inserted changes.
     * <p>
     * Warning: this method will not work properly if the first
     * row in the CDC log was inserted in a non-first CDC generation.
     *
     * @param table the table name for which to create the task.
     * @return the task containing the first row in the CDC log.
     */
    protected Task getTaskWithFirstRow(TableName table) throws ExecutionException, InterruptedException, TimeoutException {
        // Figure out the cdc$stream_id of the first change:
        Row cdcRow = driverSession.execute(QueryBuilder.select().all()
                .from(table.keyspace, table.name + "_scylla_cdc_log")).one();
        ByteBuffer streamIdBytes = cdcRow.getBytes("cdc$stream_id");

        // Get the first generation id.
        MasterCQL masterCQL = new Driver3MasterCQL(buildLibrarySession());
        GenerationId generationId = masterCQL.fetchFirstGenerationId().get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS).get();

        StreamId streamId = new StreamId(streamIdBytes);
        VNodeId vnode = streamId.getVNodeId();

        return new Task(new TaskId(generationId, vnode, table),
                Sets.newTreeSet(Collections.singleton(streamId)),
                new TaskState(new Timestamp(new Date(0)), new Timestamp(new Date()), Optional.empty()));
    }

    /**
     * Reads a first {@link RawChange} in the CDC log for the given table.
     * <p>
     * A common scenario is to insert a single row to a base table
     * and then use this method to read it back from the CDC log.
     * <p>
     * Warning: this method will not work properly if the first
     * row in the CDC log was inserted in a non-first CDC generation.
     *
     * @param table the table name for which to read the first change.
     * @return the first change in the CDC log for the given table.
     */
    protected RawChange getFirstRawChange(TableName table) throws ExecutionException, InterruptedException, TimeoutException {
        Task readTask = getTaskWithFirstRow(table);

        // Read the inserted row using WorkerCQL.
        WorkerCQL workerCQL = new Driver3WorkerCQL(buildLibrarySession());
        workerCQL.prepare(Collections.singleton(table));
        WorkerCQL.Reader reader = workerCQL.createReader(readTask).get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        return reader.nextChange().get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS).get();
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
