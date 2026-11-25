package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.session.SessionWrapper;
import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.master.GenerationMetadata;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
public class Driver4MasterCQLIT extends BaseScyllaIntegrationTest {
    static final long SCYLLA_TIMEOUT_MS = 30_000;

    @Test
    public void testMasterFetchesFirstGenerationId() throws InterruptedException, ExecutionException, TimeoutException {
        MasterCQL masterCQL = new Driver4MasterCQL(buildLibrarySession());
        Optional<GenerationId> firstGeneration =
                masterCQL.fetchFirstGenerationId().get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        assertTrue(firstGeneration.isPresent());
        assertRecent(firstGeneration.get().getGenerationStart());
    }

    @Test
    public void testMasterFetchesGenerationMetadata() throws InterruptedException, ExecutionException, TimeoutException {
        MasterCQL masterCQL = new Driver4MasterCQL(buildLibrarySession());
        GenerationId generation = masterCQL.fetchFirstGenerationId()
                .get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS).get();

        GenerationMetadata metadata = masterCQL.fetchGenerationMetadata(generation)
                .get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        assertEquals(generation.getGenerationStart(), metadata.getStart());
        // ScyllaDB 5.2 and later have only the V2 stream description table.
        Set<StreamId> expected = new HashSet<>();
        int rows = 0;
        for (Row row : driverSession.execute(SimpleStatement.newInstance(
                "SELECT streams FROM system_distributed.cdc_streams_descriptions_v2 WHERE time = ?",
                generation.getGenerationStart().toDate().toInstant()))) {
            rows++;
            row.getSet(0, ByteBuffer.class).forEach(stream -> expected.add(new StreamId(stream)));
        }
        assertTrue(rows > 1, "the generation spans several rows: " + rows);
        assertEquals(expected, metadata.getStreams());
        // A single node that never changed topology has one generation, so it has no end.
        assertFalse(metadata.getEnd().isPresent(), "the only generation is open");
    }

    @Test
    public void testMasterQueriesAreIdempotent() throws InterruptedException, ExecutionException, TimeoutException {
        try (RecordingDriver4Session session = RecordingDriver4Session.connect()) {
            MasterCQL masterCQL = new Driver4MasterCQL(session);
            GenerationId generation = masterCQL.fetchFirstGenerationId()
                    .get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS).get();
            masterCQL.fetchGenerationMetadata(generation).get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            session.assertAllIdempotent();
        }
    }

    @Test
    public void testValidateTable() throws InterruptedException, ExecutionException, TimeoutException {
        driverSession.execute("CREATE TABLE ks.cdc (p int PRIMARY KEY, v int) WITH cdc = {'enabled': true}");
        driverSession.execute("CREATE TABLE ks.plain (p int PRIMARY KEY, v int)");
        MasterCQL masterCQL = new Driver4MasterCQL(buildLibrarySession());

        assertEquals(Optional.empty(), validate(masterCQL, "ks", "cdc"));
        assertValidationError("does not have Scylla CDC enabled", validate(masterCQL, "ks", "plain"));
        assertValidationError("in Scylla cluster.", validate(masterCQL, "ks", "missing"));
        assertValidationError("missing keyspace 'nks'", validate(masterCQL, "nks", "cdc"));
    }

    @Test
    public void testFetchTableTTL() throws InterruptedException, ExecutionException, TimeoutException {
        driverSession.execute("CREATE TABLE ks.default_ttl (p int PRIMARY KEY, v int) WITH cdc = {'enabled': true}");
        driverSession.execute("CREATE TABLE ks.custom_ttl (p int PRIMARY KEY, v int) "
                + "WITH cdc = {'enabled': true, 'ttl': 3600}");
        driverSession.execute("CREATE TABLE ks.no_ttl (p int PRIMARY KEY, v int) WITH cdc = {'enabled': true, 'ttl': 0}");
        driverSession.execute("CREATE TABLE ks.plain (p int PRIMARY KEY, v int)");
        MasterCQL masterCQL = new Driver4MasterCQL(buildLibrarySession());

        assertEquals(Optional.of(86400L), ttl(masterCQL, "ks", "default_ttl"));
        assertEquals(Optional.of(3600L), ttl(masterCQL, "ks", "custom_ttl"));
        assertEquals(Optional.empty(), ttl(masterCQL, "ks", "no_ttl"));
        ExecutionException e = assertThrows(ExecutionException.class, () -> ttl(masterCQL, "ks", "plain"));
        assertTrue(e.getCause().getMessage().contains("does not have Scylla CDC enabled"), e.getCause().getMessage());
    }

    @Test
    public void testNamesThatNeedQuotes() throws InterruptedException, ExecutionException, TimeoutException {
        driverSession.execute("CREATE TABLE ks.\"order\" (p int PRIMARY KEY, v int) WITH cdc = {'enabled': true, 'ttl': 3600}");
        createVnodeKeyspace("\"select\"");
        try {
            driverSession.execute("CREATE TABLE \"select\".events (p int PRIMARY KEY, v int) WITH cdc = {'enabled': true}");
            MasterCQL masterCQL = new Driver4MasterCQL(buildLibrarySession());

            assertEquals(Optional.empty(), validate(masterCQL, "ks", "order"));
            assertEquals(Optional.of(3600L), ttl(masterCQL, "ks", "order"));
            assertEquals(Optional.empty(), validate(masterCQL, "select", "events"));
            assertEquals(Optional.of(86400L), ttl(masterCQL, "select", "events"));
            assertFalse(masterCQL.usesTablets(new TableName("select", "events")));
            // As in 3.x, an unquoted name is case-insensitive and a quoted one is exact.
            assertEquals(Optional.empty(), validate(masterCQL, "KS", "\"order\""));
            assertValidationError("in Scylla cluster.", validate(masterCQL, "ks", "\"Order\""));
        } finally {
            driverSession.execute("DROP KEYSPACE IF EXISTS \"select\"");
        }
    }

    @Test
    public void testUsesTablets() {
        MasterCQL masterCQL = new Driver4MasterCQL(buildLibrarySession());

        assertFalse(masterCQL.usesTablets(new TableName("ks", "any")));
        assertThrows(IllegalArgumentException.class, () -> masterCQL.usesTablets(new TableName("nks", "any")));
    }

    static void assertRecent(Timestamp timestamp) {
        assertTrue(timestamp.toDate().toInstant().isAfter(Instant.now().minus(Duration.ofHours(1))),
                "the timestamp is less than an hour old: " + timestamp);
    }

    /**
     * Creates a keyspace without tablets, as {@code ks} is.
     */
    private void createVnodeKeyspace(String name) {
        String create = "CREATE KEYSPACE " + name
                + " WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}";
        try {
            driverSession.execute(create + " AND tablets = {'enabled': false}");
        } catch (QueryValidationException e) {
            if (e.getMessage() == null || !e.getMessage().contains("Unknown property 'tablets'")) {
                throw e;
            }
            driverSession.execute(create);
        }
    }

    private static Optional<Throwable> validate(MasterCQL masterCQL, String keyspace, String table)
            throws InterruptedException, ExecutionException, TimeoutException {
        return masterCQL.validateTable(new TableName(keyspace, table)).get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private static void assertValidationError(String message, Optional<Throwable> error) {
        assertTrue(error.isPresent(), "validation fails with: " + message);
        assertTrue(error.get() instanceof IllegalArgumentException, error.get().toString());
        assertTrue(error.get().getMessage().contains(message), error.get().getMessage());
    }

    private static Optional<Long> ttl(MasterCQL masterCQL, String keyspace, String table)
            throws InterruptedException, ExecutionException, TimeoutException {
        return masterCQL.fetchTableTTL(new TableName(keyspace, table)).get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * A {@link Driver4Session} that records every statement {@link Driver4MasterCQL} executes.
     */
    static final class RecordingDriver4Session extends Driver4Session {
        private final List<Statement<?>> statements = new CopyOnWriteArrayList<>();
        private final CqlSession recordingSession;

        private RecordingDriver4Session(CQLConfiguration configuration) {
            super(configuration);
            recordingSession = new Recorder(super.getDriverSession());
        }

        static RecordingDriver4Session connect() {
            return new RecordingDriver4Session(CQLConfiguration.builder()
                    .addContactPoint(System.getProperty("scylla.docker.hostname"),
                            Integer.parseInt(System.getProperty("scylla.docker.port")))
                    .build());
        }

        @Override
        protected CqlSession getDriverSession() {
            return recordingSession;
        }

        void assertAllIdempotent() {
            assertFalse(statements.isEmpty(), "the master executed statements");
            for (Statement<?> statement : statements) {
                String query = statement instanceof BoundStatement
                        ? ((BoundStatement) statement).getPreparedStatement().getQuery()
                        : ((SimpleStatement) statement).getQuery();
                assertEquals(Boolean.TRUE, statement.isIdempotent(), "idempotent: " + query);
            }
        }

        private final class Recorder extends SessionWrapper implements CqlSession {
            Recorder(CqlSession delegate) {
                super(delegate);
            }

            @Override
            public <RequestT extends Request, ResultT> ResultT execute(RequestT request, GenericType<ResultT> resultType) {
                if (request instanceof Statement) {
                    statements.add((Statement<?>) request);
                }
                return super.execute(request, resultType);
            }
        }
    }
}
