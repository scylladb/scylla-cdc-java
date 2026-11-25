package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.Task;
import com.scylladb.cdc.model.worker.TaskState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Runs a test against a keyspace {@code ks} that uses vnodes.
 */
@Tag("integration")
public class BaseScyllaIntegrationTest {
    protected static final long SCYLLA_TIMEOUT_MS = 30_000;

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
            librarySession = new Driver4Session(libraryConfigurationBuilder().build());
        }
        return librarySession;
    }

    /**
     * Returns a configuration builder using the fixture's contact point.
     */
    protected CQLConfiguration.Builder libraryConfigurationBuilder() {
        return CQLConfiguration.builder().addContactPoint(hostname, port);
    }

    /**
     * Returns a task that reads the stream of the first CDC log row of {@code table}, from the
     * epoch to now. It does not work if that row is in a later CDC generation than the first.
     */
    protected Task getTaskWithFirstRow(TableName table) throws ExecutionException, InterruptedException, TimeoutException {
        Row cdcRow = driverSession.execute("SELECT \"cdc$stream_id\" FROM " + CqlIdentifier.fromInternal(table.keyspace).asCql(true)
                + "." + CqlIdentifier.fromInternal(table.name + "_scylla_cdc_log").asCql(true) + " LIMIT 1").one();
        Preconditions.checkNotNull(cdcRow, "the CDC log of %s has a row", table);
        StreamId streamId = new StreamId(cdcRow.getByteBuffer(0));

        MasterCQL masterCQL = new Driver4MasterCQL(buildLibrarySession());
        GenerationId generationId = masterCQL.fetchFirstGenerationId().get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS).get();

        return new Task(new TaskId(generationId, streamId.getVNodeId(), table),
                Sets.newTreeSet(Collections.singleton(streamId)),
                new TaskState(new Timestamp(new Date(0)), new Timestamp(new Date()), Optional.empty()));
    }

    /**
     * Reads the first change in the CDC log of {@code table} with {@link Driver4WorkerCQL}.
     */
    protected RawChange getFirstRawChange(TableName table) throws ExecutionException, InterruptedException, TimeoutException {
        Task readTask = getTaskWithFirstRow(table);
        WorkerCQL workerCQL = new Driver4WorkerCQL(buildLibrarySession());
        workerCQL.prepare(Collections.singleton(table));
        WorkerCQL.Reader reader = workerCQL.createReader(readTask).get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        return reader.nextChange().get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS).get();
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
