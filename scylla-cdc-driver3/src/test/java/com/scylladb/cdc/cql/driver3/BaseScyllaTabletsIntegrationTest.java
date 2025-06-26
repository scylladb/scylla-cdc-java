package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.*;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.Task;
import com.scylladb.cdc.model.worker.TaskState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

import static org.junit.jupiter.api.Assumptions.abort;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Tag("integration")
public class BaseScyllaTabletsIntegrationTest {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    protected static final long SCYLLA_TIMEOUT_MS = 3000;

    protected Session driverSession;
    protected Cluster driverCluster;

    private String hostname;
    private int port;
    private String scyllaVersion;
    private Driver3Session librarySession;

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

        // Create a test keyspace with tablets enabled
        try {
            driverSession.execute("CREATE KEYSPACE ks " +
                "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} " +
                "AND tablets={'initial':2};");
        } catch (Exception e) {
            if (e.getMessage().contains("Unknown property 'tablets'")) {
                abort(
                    "Test aborted: This version of Scylla doesn't support CDC with tablets. " +
                    "Error message: " + e.getMessage()
                );
            }
            throw e;
        }
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
     *
     * @param table the table name for which to create the task.
     * @return the task containing the first row in the CDC log.
     */
    protected Task getTaskWithFirstRow(TableName table) throws ExecutionException, InterruptedException, TimeoutException {
        // Figure out the cdc$stream_id of the first change:
        Row cdcRow = driverSession.execute(QueryBuilder.select().all()
                .from(table.keyspace, table.name + "_scylla_cdc_log")).one();
        ByteBuffer streamIdBytes = cdcRow.getBytes("cdc$stream_id");

        // Get the first generation id for this table
        MasterCQL masterCQL = new Driver3MasterCQL(buildLibrarySession());
        GenerationId generationId = masterCQL.fetchFirstTableGenerationId(table)
                .get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);

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

    /**
     * Attempts to create a table with CDC enabled in a tablets-mode keyspace.
     *
     * This method handles the specific case where a Scylla version doesn't support
     * CDC with tablets mode by detecting the specific error message and aborting the test
     * rather than letting it fail. This approach allows the test suite to run on both
     * Scylla versions that support CDC with tablets and those that don't.
     *
     * If the table creation fails for any other reason, the original exception is rethrown
     * so that the test fails normally, indicating a real issue rather than a feature limitation.
     *
     * @param query The CREATE TABLE query to execute
     * @throws InvalidQueryException if the table cannot be created for reasons other than
     *         CDC with tablets compatibility
     */
    public void tryCreateTable(String query) throws InvalidQueryException {
        try {
            driverSession.execute(query);
        } catch (InvalidQueryException e) {
            // Check if this is the specific exception about CDC logs in tablet mode
            if (e.getMessage().contains("Cannot create CDC log for a table") &&
                e.getMessage().contains("because keyspace uses tablets")) {
                abort(
                    "Test aborted: This version of Scylla doesn't support CDC with tablets. " +
                    "Error message: " + e.getMessage()
                );
            }
            throw e;
        }
    }
}
