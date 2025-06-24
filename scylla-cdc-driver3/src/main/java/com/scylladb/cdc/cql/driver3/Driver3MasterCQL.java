package com.scylladb.cdc.cql.driver3;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.column;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Ordering;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.scylladb.cdc.cql.BaseMasterCQL;
import com.scylladb.cdc.model.TableName;

public final class Driver3MasterCQL extends BaseMasterCQL {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    private final Session session;

    /**
     * Enum representing the state of a CDC stream for a specific timestamp.
     * These match the values used in the system.cdc_streams table.
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

    // (Streams description table V2)
    //
    // PreparedStatements for querying in clusters with
    // system_distributed.cdc_generation_timestamps
    // and system_distributed.cdc_streams_descriptions_v2 tables.
    private PreparedStatement fetchSmallestGenerationAfterStmt;
    private PreparedStatement fetchStreamsStmt;
    private boolean foundRewritten = false;

    // (Streams description table V1)
    //
    // PreparedStatements for querying in clusters WITHOUT
    // system_distributed.cdc_generation_timestamps
    // and system_distributed.cdc_streams_descriptions_v2 tables.
    private PreparedStatement legacyFetchSmallestGenerationAfterStmt;
    private PreparedStatement legacyFetchStreamsStmt;
    private PreparedStatement fetchSmallestTableGenerationAfterStmt;
    private PreparedStatement fetchTableStreamsStmt;

    public Driver3MasterCQL(Driver3Session session) {
        this.session = Preconditions.checkNotNull(session).getDriverSession();
    }

    private CompletableFuture<Boolean> fetchShouldQueryLegacyTables() {
        // Decide if we should query "Streams description table V1" (legacy)

        boolean hasNewTables = session.getCluster().getMetadata().getKeyspace("system_distributed")
                .getTable("cdc_generation_timestamps") != null;
        boolean hasLegacyTables = session.getCluster().getMetadata().getKeyspace("system_distributed")
                .getTable("cdc_streams_descriptions") != null;

        // Simple cases when there are only
        // tables from one version:

        if (hasLegacyTables && !hasNewTables) {
            logger.atFine().log("Using legacy (V1) streams description table, as a newer (V2) table was not found.");
            return CompletableFuture.completedFuture(true);
        }

        if (!hasLegacyTables && hasNewTables) {
            logger.atFine().log("Using new (V2) streams description table, as a legacy (V1) table was not found.");
            return CompletableFuture.completedFuture(false);
        }

        if (!hasLegacyTables && !hasNewTables) {
            // No stream description tables found!
            CompletableFuture<Boolean> exceptionalFuture = new CompletableFuture<>();
            exceptionalFuture.completeExceptionally(new IllegalStateException("Could not find any Scylla CDC stream " +
                    "description tables (either streams description table V1 or V2). Make sure you have Scylla CDC enabled."));
            return exceptionalFuture;
        }

        // By now we know that there are both "Streams description table V1"
        // and "Streams description table V2" present in the cluster.
        //
        // We should use "Streams description table V2" only after a
        // rewrite has completed:
        // https://github.com/scylladb/scylla/blob/master/docs/design-notes/cdc.md#streams-description-table-v1-and-rewriting

        if (foundRewritten) {
            // If we found a "rewritten" row, that means that
            // we can use the "Streams description table V2".
            logger.atFiner().log("Using new (V2) streams description table, because a 'rewritten' row was found previously.");
            return CompletableFuture.completedFuture(false);
        }

        // We haven't seen a rewritten row yet. Do a query
        // to check if it exists now.

        return executeOne(getFetchRewritten()).thenApply(fetchedRewritten -> {
            if (fetchedRewritten.isPresent()) {
                // There is a "rewritten" row.
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
    }

    private CompletableFuture<PreparedStatement> getLegacyFetchSmallestGenerationAfter() {
        if (legacyFetchSmallestGenerationAfterStmt != null) {
            return CompletableFuture.completedFuture(legacyFetchSmallestGenerationAfterStmt);
        } else {
            ListenableFuture<PreparedStatement> prepareStatement = session.prepareAsync(
                    select().min(column("time")).from("system_distributed", "cdc_streams_descriptions")
                            .where(gt("time", bindMarker())).allowFiltering()
            );
            return FutureUtils.convert(prepareStatement).thenApply(preparedStatement -> {
                legacyFetchSmallestGenerationAfterStmt = preparedStatement;
                return preparedStatement;
            });
        }
    }

    private CompletableFuture<PreparedStatement> getFetchSmallestGenerationAfter() {
        if (fetchSmallestGenerationAfterStmt != null) {
            return CompletableFuture.completedFuture(fetchSmallestGenerationAfterStmt);
        } else {
            ListenableFuture<PreparedStatement> prepareStatement = session.prepareAsync(
                    select().min(column("time")).from("system_distributed", "cdc_generation_timestamps")
                            .where(eq("key", "timestamps")).and(gt("time", bindMarker()))
            );
            return FutureUtils.convert(prepareStatement).thenApply(preparedStatement -> {
                fetchSmallestGenerationAfterStmt = preparedStatement;
                return preparedStatement;
            });
        }
    }

    private CompletableFuture<PreparedStatement> getFetchSmallestTableGenerationAfter() {
        if (fetchSmallestTableGenerationAfterStmt != null) {
            return CompletableFuture.completedFuture(fetchSmallestTableGenerationAfterStmt);
        } else {
            ListenableFuture<PreparedStatement> prepareStatement = session.prepareAsync(
                    select().min(column("timestamp")).from("system", "cdc_timestamps")
                            .where(eq("keyspace_name", bindMarker()))
                            .and(eq("table_name", bindMarker()))
                            .and(gt("timestamp", bindMarker()))
                            .orderBy(QueryBuilder.asc("timestamp"))
                            .limit(1)
            );
            return FutureUtils.convert(prepareStatement).thenApply(preparedStatement -> {
                fetchSmallestTableGenerationAfterStmt = preparedStatement;
                return preparedStatement;
            });
        }
    }

    private CompletableFuture<PreparedStatement> getLegacyFetchStreams() {
        if (legacyFetchStreamsStmt != null) {
            return CompletableFuture.completedFuture(legacyFetchStreamsStmt);
        } else {
            ListenableFuture<PreparedStatement> prepareStatement = session.prepareAsync(
                    select().column("streams").from("system_distributed", "cdc_streams_descriptions")
                            .where(eq("time", bindMarker())).allowFiltering()
            );
            return FutureUtils.convert(prepareStatement).thenApply(preparedStatement -> {
                legacyFetchStreamsStmt = preparedStatement;
                return preparedStatement;
            });
        }
    }

    private CompletableFuture<PreparedStatement> getFetchStreams() {
        if (fetchStreamsStmt != null) {
            return CompletableFuture.completedFuture(fetchStreamsStmt);
        } else {
            ListenableFuture<PreparedStatement> prepareStatement = session.prepareAsync(
                    select().column("streams").from("system_distributed", "cdc_streams_descriptions_v2")
                            .where(eq("time", bindMarker()))
            );
            return FutureUtils.convert(prepareStatement).thenApply(preparedStatement -> {
                fetchStreamsStmt = preparedStatement;
                return preparedStatement;
            });
        }
    }

    private CompletableFuture<PreparedStatement> getFetchTableStreams() {
        if (fetchTableStreamsStmt != null) {
            return CompletableFuture.completedFuture(fetchTableStreamsStmt);
        } else {
            ListenableFuture<PreparedStatement> prepareStatement = session.prepareAsync(
                    select().column("stream_id").from("system", "cdc_streams")
                            .where(eq("keyspace_name", bindMarker()))
                            .and(eq("table_name", bindMarker()))
                            .and(eq("timestamp", bindMarker()))
                            .and(eq("stream_state", StreamState.CURRENT.getValue()))
            );
            return FutureUtils.convert(prepareStatement).thenApply(preparedStatement -> {
                fetchTableStreamsStmt = preparedStatement;
                return preparedStatement;
            });
        }
    }

    private Statement getFetchRewritten() {
        return select().from("system", "cdc_local")
                .where(eq("key", "rewritten"));
    }

    private ConsistencyLevel computeCL() {
        return session.getCluster().getMetadata().getAllHosts().size() > 1 ? ConsistencyLevel.QUORUM
                : ConsistencyLevel.ONE;
    }

    private void consumeOneResult(ResultSet rs, CompletableFuture<Optional<Row>> result) {
        int availCount = rs.getAvailableWithoutFetching();
        if (availCount == 0) {
            if (rs.isFullyFetched()) {
                result.complete(Optional.empty());
            } else {
                Futures.addCallback(rs.fetchMoreResults(), new FutureCallback<ResultSet>() {

                    @Override
                    public void onSuccess(ResultSet rs) {
                        consumeOneResult(rs, result);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        result.completeExceptionally(t);
                    }
                }, MoreExecutors.directExecutor());
            }
        } else {
            assert (availCount == 1);
            result.complete(Optional.of(rs.one()));
        }
    }

    private void consumeManyResults(ResultSet rs, Collection<Row> alreadyFetched,
                                    CompletableFuture<Collection<Row>> result) {
        int availableWithoutFetching = rs.getAvailableWithoutFetching();
        if (availableWithoutFetching == 0) {
            if (rs.isFullyFetched()) {
                result.complete(alreadyFetched);
            } else {
                Futures.addCallback(rs.fetchMoreResults(), new FutureCallback<ResultSet>() {

                    @Override
                    public void onSuccess(ResultSet rsNew) {
                        consumeManyResults(rsNew, alreadyFetched, result);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        result.completeExceptionally(t);
                    }
                }, MoreExecutors.directExecutor());
            }
        } else {
            for (int i = 0; i < availableWithoutFetching; i++) {
                alreadyFetched.add(rs.one());
            }
            consumeManyResults(rs, alreadyFetched, result);
        }
    }

    private CompletableFuture<Optional<Row>> executeOne(Statement stmt) {
        CompletableFuture<Optional<Row>> result = new CompletableFuture<>();
        ResultSetFuture future = session.executeAsync(stmt.setConsistencyLevel(computeCL()));
        Futures.addCallback(future, new FutureCallback<ResultSet>() {

            @Override
            public void onSuccess(ResultSet rs) {
                consumeOneResult(rs, result);
            }

            @Override
            public void onFailure(Throwable t) {
                result.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
        return result;
    }

    private CompletableFuture<Collection<Row>> executeMany(Statement stmt) {
        CompletableFuture<Collection<Row>> result = new CompletableFuture<>();
        ResultSetFuture future = session.executeAsync(stmt.setConsistencyLevel(computeCL()));
        Futures.addCallback(future, new FutureCallback<ResultSet>() {

            @Override
            public void onSuccess(ResultSet rs) {
                consumeManyResults(rs, new ArrayList<>(), result);
            }

            @Override
            public void onFailure(Throwable t) {
                result.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
        return result;
    }

    @Override
    protected CompletableFuture<Optional<Date>> fetchSmallestGenerationAfter(Date after) {
        return fetchShouldQueryLegacyTables().thenCompose(shouldQueryLegacyTables -> {
            if (shouldQueryLegacyTables) {
                return getLegacyFetchSmallestGenerationAfter().thenCompose(statement ->
                        executeOne(statement.bind(after)).thenApply(o -> o.map(r -> r.getTimestamp(0))));
            } else {
                return getFetchSmallestGenerationAfter().thenCompose(statement ->
                        executeOne(statement.bind(after)).thenApply(o -> o.map(r -> r.getTimestamp(0))));
            }
        });
    }

    @Override
    protected CompletableFuture<Optional<Date>> fetchSmallestTableGenerationAfter(TableName tableName, Date after) {
        return getFetchSmallestTableGenerationAfter().thenCompose(statement ->
                executeOne(statement.bind(tableName.keyspace, tableName.name, after)).thenApply(o -> o.map(r -> r.getTimestamp(0))));
    }

    @Override
    protected CompletableFuture<Set<ByteBuffer>> fetchStreamsForTableGeneration(TableName tableName, Date generationStart) {
        return getFetchTableStreams().thenCompose(statement ->
            executeMany(statement.bind(tableName.keyspace, tableName.name, generationStart)).thenApply(
                rows -> rows.stream()
                        .map(row -> row.getBytes("stream_id"))
                        .collect(Collectors.toSet())
            )
        );
    }

    @Override
    protected CompletableFuture<Set<ByteBuffer>> fetchStreamsForGeneration(Date generationStart) {
        return fetchShouldQueryLegacyTables().thenCompose(shouldQueryLegacyTables -> {
            if (shouldQueryLegacyTables) {
                return getLegacyFetchStreams().thenCompose(statement ->
                        executeOne(statement.bind(generationStart)).thenApply(o -> o.get().getSet(0, ByteBuffer.class)));
            } else {
                return getFetchStreams().thenCompose(statement -> executeMany(statement.bind(generationStart))
                        .thenApply(o -> o.stream().flatMap(r -> r.getSet(0, ByteBuffer.class).stream()).collect(Collectors.toSet())));
            }
        });
    }

    @Override
    public CompletableFuture<Optional<Long>> fetchTableTTL(TableName tableName) {
        return Driver3CommonCQL.fetchTableTTL(session, tableName);
    }

    @Override
    public CompletableFuture<Optional<Throwable>> validateTable(TableName table) {
        KeyspaceMetadata keyspaceMetadata = session.getCluster().getMetadata().getKeyspace(table.keyspace);
        if (keyspaceMetadata == null) {
            return CompletableFuture.completedFuture(Optional.of(new IllegalArgumentException(
                    String.format("Did not find table '%s.%s' in Scylla cluster - missing keyspace '%s'.",
                            table.keyspace, table.name, table.keyspace))));
        }

        TableMetadata tableMetadata = keyspaceMetadata.getTable(table.name);
        if (tableMetadata == null) {
            return CompletableFuture.completedFuture(Optional.of(new IllegalArgumentException(
                    String.format("Did not find table '%s.%s' in Scylla cluster.",
                            table.keyspace, table.name))));
        }

        if (!tableMetadata.getOptions().isScyllaCDC()) {
            return CompletableFuture.completedFuture(Optional.of(new IllegalArgumentException(
                    String.format("The table '%s.%s' does not have Scylla CDC enabled.",
                            table.keyspace, table.name))));
        }

        return CompletableFuture.completedFuture(Optional.empty());
    }
}
