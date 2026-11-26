package com.scylladb.cdc.cql.driver4;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.cql.BaseMasterCQL;
import com.scylladb.cdc.model.TableName;

/**
 * Reads CDC generations and their streams with Java driver 4.x, the counterpart of
 * {@code Driver3MasterCQL}.
 * <p>
 * Driver 4.x leaves {@code system_*} keyspaces out of its schema metadata, so the stream
 * description tables are looked up in {@code system_schema.tables} instead.
 */
public final class Driver4MasterCQL extends BaseMasterCQL {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    /**
     * The state of a CDC stream at a timestamp, as stored in {@code system.cdc_streams}.
     */
    public enum StreamState {
        CURRENT(0), // the stream is active in this timestamp
        CLOSED(1), // the stream was active in the previous timestamp and it's not active in this timestamp
        OPENED(2); // the stream is a new stream opened in this timestamp

        private final int value;

        StreamState(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    // Streams description table V2.
    private static final String FETCH_SMALLEST_GENERATION_AFTER =
            "SELECT min(time) FROM system_distributed.cdc_generation_timestamps WHERE key = 'timestamps' AND time > ?";
    private static final String FETCH_STREAMS =
            "SELECT streams FROM system_distributed.cdc_streams_descriptions_v2 WHERE time = ?";

    // Streams description table V1 (legacy).
    private static final String LEGACY_FETCH_SMALLEST_GENERATION_AFTER =
            "SELECT min(time) FROM system_distributed.cdc_streams_descriptions WHERE time > ? ALLOW FILTERING";
    private static final String LEGACY_FETCH_STREAMS =
            "SELECT streams FROM system_distributed.cdc_streams_descriptions WHERE time = ? ALLOW FILTERING";

    // Tablets: per-table generations.
    private static final String FETCH_SMALLEST_TABLE_GENERATION_AFTER =
            "SELECT min(timestamp) FROM system.cdc_timestamps WHERE keyspace_name = ? AND table_name = ? "
                    + "AND timestamp > ? ORDER BY timestamp ASC LIMIT 1";
    private static final String FETCH_TABLE_STREAMS =
            "SELECT stream_id FROM system.cdc_streams WHERE keyspace_name = ? AND table_name = ? "
                    + "AND timestamp = ? AND stream_state = " + StreamState.CURRENT.getValue();

    private static final String FETCH_REWRITTEN = "SELECT * FROM system.cdc_local WHERE key = 'rewritten'";
    private static final String FETCH_STREAM_DESCRIPTION_TABLES =
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'system_distributed' "
                    + "AND table_name IN ('cdc_generation_timestamps', 'cdc_streams_descriptions')";

    private final CqlSession session;
    private final Map<String, PreparedStatement> preparedStatements = new ConcurrentHashMap<>();
    private volatile boolean foundRewritten = false;

    public Driver4MasterCQL(Driver4Session session) {
        this.session = Preconditions.checkNotNull(session).getDriverSession();
    }

    private CompletableFuture<Boolean> fetchShouldQueryLegacyTables() {
        // Decide if we should query "Streams description table V1" (legacy)
        return executeMany(SimpleStatement.newInstance(FETCH_STREAM_DESCRIPTION_TABLES)).thenCompose(rows -> {
            Set<String> tables = rows.stream().map(row -> row.getString(0)).collect(Collectors.toSet());
            boolean hasNewTables = tables.contains("cdc_generation_timestamps");
            boolean hasLegacyTables = tables.contains("cdc_streams_descriptions");

            if (hasLegacyTables && !hasNewTables) {
                logger.atFine().log("Using legacy (V1) streams description table, as a newer (V2) table was not found.");
                return CompletableFuture.completedFuture(true);
            }

            if (!hasLegacyTables && hasNewTables) {
                logger.atFine().log("Using new (V2) streams description table, as a legacy (V1) table was not found.");
                return CompletableFuture.completedFuture(false);
            }

            if (!hasLegacyTables) {
                CompletableFuture<Boolean> exceptionalFuture = new CompletableFuture<>();
                exceptionalFuture.completeExceptionally(new IllegalStateException("Could not find any Scylla CDC stream " +
                        "description tables (either streams description table V1 or V2). Make sure you have Scylla CDC enabled."));
                return exceptionalFuture;
            }

            // Both tables exist. Use V2 only after the rewrite has completed:
            // https://github.com/scylladb/scylla/blob/master/docs/design-notes/cdc.md#streams-description-table-v1-and-rewriting
            if (foundRewritten) {
                logger.atFiner().log("Using new (V2) streams description table, because a 'rewritten' row was found previously.");
                return CompletableFuture.completedFuture(false);
            }

            return executeOne(SimpleStatement.newInstance(FETCH_REWRITTEN)).thenApply(fetchedRewritten -> {
                if (fetchedRewritten.isPresent()) {
                    foundRewritten = true;
                    logger.atInfo().log("Found a 'rewritten' row. Will use new (V2) streams description table from now on.");
                    return false;
                } else {
                    logger.atWarning().log("Using legacy (V1) streams description table, even though newer (V2) table was found, but " +
                            "a 'rewritten' row is still missing. This might mean that the rewriting process is still pending or you have " +
                            "disabled streams description rewriting - in that case the library will not switch to the new (V2) table " +
                            "until it discovers a 'rewritten' row. Read more at: " +
                            "https://github.com/scylladb/scylla/blob/master/docs/design-notes/cdc.md#streams-description-table-v1-and-rewriting");
                    return true;
                }
            });
        });
    }

    private CompletableFuture<PreparedStatement> prepare(String query) {
        PreparedStatement prepared = preparedStatements.get(query);
        if (prepared != null) {
            return CompletableFuture.completedFuture(prepared);
        }
        return session.prepareAsync(query).toCompletableFuture().thenApply(statement -> {
            preparedStatements.put(query, statement);
            return statement;
        });
    }

    private ConsistencyLevel computeCL() {
        return session.getMetadata().getNodes().size() > 1 ? DefaultConsistencyLevel.QUORUM
                : DefaultConsistencyLevel.ONE;
    }

    /**
     * Returns the first row, fetching pages until one has a row.
     */
    private static CompletionStage<Optional<Row>> firstRow(AsyncResultSet rs) {
        Row row = rs.one();
        if (row != null) {
            return CompletableFuture.completedFuture(Optional.of(row));
        }
        return rs.hasMorePages()
                ? rs.fetchNextPage().thenCompose(Driver4MasterCQL::firstRow)
                : CompletableFuture.completedFuture(Optional.empty());
    }

    private static CompletionStage<Collection<Row>> allRows(AsyncResultSet rs, Collection<Row> alreadyFetched) {
        for (Row row : rs.currentPage()) {
            alreadyFetched.add(row);
        }
        return rs.hasMorePages()
                ? rs.fetchNextPage().thenCompose(next -> allRows(next, alreadyFetched))
                : CompletableFuture.completedFuture(alreadyFetched);
    }

    private CompletionStage<AsyncResultSet> execute(Statement<?> stmt) {
        // Every query here is a read, so the driver may retry it on another node.
        return session.executeAsync(stmt.setConsistencyLevel(computeCL()).setIdempotent(true));
    }

    private CompletableFuture<Optional<Row>> executeOne(Statement<?> stmt) {
        return execute(stmt).thenCompose(Driver4MasterCQL::firstRow).toCompletableFuture();
    }

    private CompletableFuture<Collection<Row>> executeMany(Statement<?> stmt) {
        return execute(stmt).thenCompose(rs -> allRows(rs, new ArrayList<>())).toCompletableFuture();
    }

    /**
     * Reads a {@code min(...)} result, which is one row with a null value when nothing matched.
     */
    private static Optional<Date> minTimestamp(Optional<Row> row) {
        return row.flatMap(r -> Optional.ofNullable(r.getInstant(0))).map(Date::from);
    }

    @Override
    protected CompletableFuture<Optional<Date>> fetchSmallestGenerationAfter(Date after) {
        return fetchShouldQueryLegacyTables().thenCompose(shouldQueryLegacyTables ->
                prepare(shouldQueryLegacyTables ? LEGACY_FETCH_SMALLEST_GENERATION_AFTER : FETCH_SMALLEST_GENERATION_AFTER)
                        .thenCompose(statement -> executeOne(statement.bind(after.toInstant())))
                        .thenApply(Driver4MasterCQL::minTimestamp));
    }

    // Binds the stored name that validateTable resolves to: "Events" quoted is Events, KS unquoted is ks.
    private static String internalName(String name) {
        return Driver4CommonCQL.identifier(name).asInternal();
    }

    @Override
    protected CompletableFuture<Optional<Date>> fetchSmallestTableGenerationAfter(TableName tableName, Date after) {
        return prepare(FETCH_SMALLEST_TABLE_GENERATION_AFTER)
                .thenCompose(statement -> executeOne(statement.bind(
                        internalName(tableName.keyspace), internalName(tableName.name), after.toInstant())))
                .thenApply(Driver4MasterCQL::minTimestamp);
    }

    @Override
    protected CompletableFuture<Set<ByteBuffer>> fetchStreamsForTableGeneration(TableName tableName, Date generationStart) {
        return prepare(FETCH_TABLE_STREAMS).thenCompose(statement ->
                executeMany(statement.bind(internalName(tableName.keyspace), internalName(tableName.name),
                        generationStart.toInstant())).thenApply(
                        rows -> rows.stream()
                                .map(row -> row.getByteBuffer("stream_id"))
                                .collect(Collectors.toSet())));
    }

    @Override
    protected CompletableFuture<Set<ByteBuffer>> fetchStreamsForGeneration(Date generationStart) {
        return fetchShouldQueryLegacyTables().thenCompose(shouldQueryLegacyTables -> {
            if (shouldQueryLegacyTables) {
                return prepare(LEGACY_FETCH_STREAMS).thenCompose(statement ->
                        executeOne(statement.bind(generationStart.toInstant()))
                                .thenApply(o -> o.get().getSet(0, ByteBuffer.class)));
            } else {
                return prepare(FETCH_STREAMS).thenCompose(statement ->
                        executeMany(statement.bind(generationStart.toInstant()))
                                .thenApply(o -> o.stream()
                                        .flatMap(r -> r.getSet(0, ByteBuffer.class).stream())
                                        .collect(Collectors.toSet())));
            }
        });
    }

    @Override
    public CompletableFuture<Optional<Long>> fetchTableTTL(TableName tableName) {
        return Driver4CommonCQL.fetchTableTTL(session, tableName);
    }

    @Override
    public CompletableFuture<Optional<Throwable>> validateTable(TableName table) {
        Optional<KeyspaceMetadata> keyspaceMetadata = Driver4CommonCQL.keyspace(session, table);
        if (!keyspaceMetadata.isPresent()) {
            return CompletableFuture.completedFuture(Optional.of(new IllegalArgumentException(
                    String.format("Did not find table '%s.%s' in Scylla cluster - missing keyspace '%s'.",
                            table.keyspace, table.name, table.keyspace))));
        }

        Optional<TableMetadata> tableMetadata = Driver4CommonCQL.table(keyspaceMetadata.get(), table);
        if (!tableMetadata.isPresent()) {
            return CompletableFuture.completedFuture(Optional.of(new IllegalArgumentException(
                    String.format("Did not find table '%s.%s' in Scylla cluster.",
                            table.keyspace, table.name))));
        }

        if (!Driver4CommonCQL.cdcExtension(tableMetadata.get()).isPresent()) {
            return CompletableFuture.completedFuture(Optional.of(new IllegalArgumentException(
                    String.format("The table '%s.%s' does not have Scylla CDC enabled.",
                            table.keyspace, table.name))));
        }

        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public Boolean usesTablets(TableName table) {
        return Driver4CommonCQL.keyspace(session, table)
                .orElseThrow(() -> new IllegalArgumentException(
                        String.format("Did not find table '%s.%s' in Scylla cluster - missing keyspace '%s'.",
                                table.keyspace, table.name, table.keyspace)))
                .isUsingTablets();
    }
}
