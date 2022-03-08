package com.scylladb.cdc.cql.driver3;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.Task;

public final class Driver3WorkerCQL implements WorkerCQL {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final Session session;
    private final Map<TableName, PreparedStatement> preparedStmts = new HashMap<>();
    private final ConsistencyLevel consistencyLevel;

    public Driver3WorkerCQL(Driver3Session session) {
        this.session = Preconditions.checkNotNull(session).getDriverSession();
        this.consistencyLevel = session.getConsistencyLevel();
    }

    private static final class PreparedResult {
        public final TableName table;
        public final PreparedStatement stmt;

        public PreparedResult(TableName table, PreparedStatement stmt) {
            this.table = Preconditions.checkNotNull(table);
            this.stmt = Preconditions.checkNotNull(stmt);
        }
    }

    private static RegularStatement getStmt(TableName table) {
        return select().all().from(quoteIfNecessary(table.keyspace), quoteIfNecessary(table.name + "_scylla_cdc_log"))
                .where(in(quoteIfNecessary("cdc$stream_id"), bindMarker()))
                .and(gt(quoteIfNecessary("cdc$time"), bindMarker()))
                .and(lte(quoteIfNecessary("cdc$time"), bindMarker()));
    }

    private CompletableFuture<PreparedResult> prepare(TableName table) {
        CompletableFuture<PreparedResult> result = new CompletableFuture<>();
        Futures.addCallback(session.prepareAsync(getStmt(table)), new FutureCallback<PreparedStatement>() {

            @Override
            public void onSuccess(PreparedStatement r) {
                result.complete(new PreparedResult(table, r));
            }

            @Override
            public void onFailure(Throwable t) {
                result.completeExceptionally(t);
            }
        });
        return result;
    }

    @Override
    public void prepare(Set<TableName> tables) throws InterruptedException, ExecutionException {
        @SuppressWarnings("unchecked")
        CompletableFuture<PreparedResult>[] futures = tables.stream().filter(t -> !preparedStmts.containsKey(t))
                .map(this::prepare).toArray(n -> new CompletableFuture[n]);
        CompletableFuture.allOf(futures).get();
        for (CompletableFuture<PreparedResult> f : futures) {
            PreparedResult r = f.get();
            preparedStmts.put(r.table, r.stmt);
        }
    }

    private final class Driver3Reader implements Reader {
        private final PreparedStatement stmt;
        private final Task task;

        private volatile ChangeSchema schema = null;

        // When there is lastChangeId, Driver3Reader
        // will first read stream from lastChangeId
        // skipping some prefix of it, then it will
        // read entire streams from a stream one past
        // stream in lastChangeId.
        //
        // If there was none lastChangeId, Driver3Reader
        // will only read from entireStreamsResult.
        private volatile ResultSet partialStreamResult = null;
        private volatile ResultSet entireStreamsResult = null;

        private volatile boolean finishedReadingPartialStreamResult = false;

        public Driver3Reader(PreparedStatement stmt, Task task) {
            this.stmt = stmt;
            this.task = task;

            if (!task.state.getLastConsumedChangeId().isPresent()) {
                finishedReadingPartialStreamResult = true;
            }
        }

        private CompletableFuture<ResultSet> executeAsync(Statement statement) {
            // Converts from ResultSetFuture to CompletableFuture<ResultSet>
            ResultSetFuture future = session.executeAsync(statement);
            CompletableFuture<ResultSet> result = new CompletableFuture<>();

            Futures.addCallback(future, new FutureCallback<ResultSet>() {
                @Override
                public void onSuccess(ResultSet rs) {
                    result.complete(rs);
                }

                @Override
                public void onFailure(Throwable t) {
                    result.completeExceptionally(t);
                }
            });

            return result;
        }

        private CompletableFuture<Optional<Row>> fetchRow(ResultSet rs) {
            if (rs.getAvailableWithoutFetching() == 0) {
                if (rs.isFullyFetched()) {
                    return CompletableFuture.completedFuture(Optional.empty());
                } else {
                    CompletableFuture<Optional<Row>> result = new CompletableFuture<>();
                    Futures.addCallback(rs.fetchMoreResults(), new FutureCallback<ResultSet>() {
                        @Override
                        public void onSuccess(ResultSet ignored) {
                            // It turns out you don't have to use the ResultSet
                            // returned by rs.fetchMoreResults(), just use the
                            // original rs.
                            fetchRow(rs).thenAccept(result::complete);
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            result.completeExceptionally(t);
                        }
                    });
                    return result;
                }
            } else {
                return CompletableFuture.completedFuture(Optional.of(rs.one()));
            }
        }

        private RawChange translateRowToRawChange(Row row) throws Driver3SchemaFactory.UnresolvableSchemaInconsistencyException {
            // The first assumption here is that we can
            // build the schema only once and it won't
            // change during the lifetime of this
            // reader (during a single query window):
            // the underlying PreparedStatement.
            //
            // We rely on the implementation of
            // Java Driver and the implemented
            // native protocol version (V4) - set in Driver3Session,
            // that for a single PreparedStatement
            // each row will have the same
            // schema.
            //
            // This assumption is verified in integration tests:
            // Driver3WorkerCQLIT#testPreparedStatementSameSchemaBetweenPages
            // and Driver3WorkerCQLIT#testPreparedStatementOldSchemaAfterAlter.
            //
            // The second assumption is that
            // between a time that the CDC log query
            // was prepared (and therefore a CDC schema was
            // returned by the preparing of the query)
            // and fetching of the base table schema (now)
            // there was at most a single schema change.
            if (schema == null) {
                schema = Driver3SchemaFactory.getChangeSchema(row, session.getCluster().getMetadata());
            }
            return new Driver3RawChange(row, schema);
        }

        private void startReadingPartialStreamResult(CompletableFuture<Optional<RawChange>> result) {
            ChangeId lastConsumedChangeId = task.state.getLastConsumedChangeId().get();

            UUID windowStart = lastConsumedChangeId.getChangeTime().getUUID();
            if (task.state.getWindowStart().compareTo(windowStart) > 0) {
                windowStart = task.state.getWindowStart();
            }

            Statement statement = stmt
                    .bind(Collections.singletonList(lastConsumedChangeId.getStreamId().getValue()),
                            windowStart, task.state.getWindowEnd())
                    .setConsistencyLevel(consistencyLevel);

            executeAsync(statement).thenAccept(rs -> {
                partialStreamResult = rs;
                findNext(result);
            }).exceptionally(ex -> {
                result.completeExceptionally(ex);
                return null;
            });
        }

        private void findNextInPartialStreamResult(CompletableFuture<Optional<RawChange>> result) {
            fetchRow(partialStreamResult).thenAccept(row -> {
                if (row.isPresent()) {
                    try {
                        result.complete(Optional.of(translateRowToRawChange(row.get())));
                    } catch (Driver3SchemaFactory.UnresolvableSchemaInconsistencyException e) {
                        result.completeExceptionally(e);
                    }
                } else {
                    finishedReadingPartialStreamResult = true;
                    findNext(result);
                }
            }).exceptionally(ex -> {
                result.completeExceptionally(ex);
                return null;
            });
        }

        private void startReadingEntireStreamsResult(CompletableFuture<Optional<RawChange>> result) {
            Stream<StreamId> streams = task.streams.stream();

            if (task.state.getLastConsumedChangeId().isPresent()) {
                ChangeId lastConsumedChangeId = task.state.getLastConsumedChangeId().get();
                streams = streams.filter(s -> s.compareTo(lastConsumedChangeId.getStreamId()) > 0);
            }

            Statement statement = stmt
                            .bind(streams.map(StreamId::getValue).collect(Collectors.toList()),
                                    task.state.getWindowStart(), task.state.getWindowEnd())
                            .setConsistencyLevel(consistencyLevel);

            executeAsync(statement).thenAccept(rs -> {
                entireStreamsResult = rs;
                findNext(result);
            }).exceptionally(ex -> {
                result.completeExceptionally(ex);
                return null;
            });
        }

        private void findNextInEntireStreamsResult(CompletableFuture<Optional<RawChange>> result) {
            fetchRow(entireStreamsResult).thenAccept(row -> {
                if (row.isPresent()) {
                    try {
                        result.complete(Optional.of(translateRowToRawChange(row.get())));
                    } catch (Driver3SchemaFactory.UnresolvableSchemaInconsistencyException e) {
                        result.completeExceptionally(e);
                    }
                } else {
                    result.complete(Optional.empty());
                }
            }).exceptionally(ex -> {
                result.completeExceptionally(ex);
                return null;
            });
        }

        public void findNext(CompletableFuture<Optional<RawChange>> result) {
            if (!finishedReadingPartialStreamResult) {
                // Read from partialStreamResult
                if (partialStreamResult == null) {
                    startReadingPartialStreamResult(result);
                } else {
                    findNextInPartialStreamResult(result);
                }
            } else {
                // Read from entireStreamsResult
                if (entireStreamsResult == null) {
                    startReadingEntireStreamsResult(result);
                } else {
                    findNextInEntireStreamsResult(result);
                }
            }
        }

        @Override
        public CompletableFuture<Optional<RawChange>> nextChange() {
            CompletableFuture<Optional<RawChange>> result = new CompletableFuture<>();
            findNext(result);
            return result;
        }
    }

    @Override
    public CompletableFuture<Reader> createReader(Task task) {
        PreparedStatement stmt = preparedStmts.get(task.id.getTable());
        Preconditions.checkNotNull(stmt);
        return CompletableFuture.completedFuture(new Driver3Reader(stmt, task));
    }

    @Override
    public CompletableFuture<Optional<Long>> fetchTableTTL(TableName tableName) {
        return Driver3CommonCQL.fetchTableTTL(session, tableName);
    }
}
