package com.scylladb.cdc.cql.driver3;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.Task;

public class Driver3WorkerCQL implements WorkerCQL {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final Session session;
    private final Map<TableName, PreparedStatement> preparedStmts = new ConcurrentHashMap<>();
    // Lazily populated when fetchFirstChangeTime is called. An empty ConcurrentHashMap
    // has negligible overhead, so eager allocation is acceptable.
    private final Map<TableName, PreparedStatement> probeStmts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TableName, CompletableFuture<PreparedStatement>> probeFutures = new ConcurrentHashMap<>();
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
                .where(eq(quoteIfNecessary("cdc$stream_id"), bindMarker()))
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
        }, MoreExecutors.directExecutor());
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

    protected class Driver3Reader implements Reader {

        protected volatile ResultSet rs;
        private volatile ChangeSchema schema;
        protected final Optional<ChangeId> lastChangeId;
        private volatile boolean shouldTryRecreateSchema = true;
        private volatile int lastPageColDefsHashcode = 0;

        public Driver3Reader(ResultSet rs, Optional<ChangeId> lastChangeId) {
            this.rs = Preconditions.checkNotNull(rs);
            this.lastChangeId = Preconditions.checkNotNull(lastChangeId);
        }

        protected void findNext(CompletableFuture<Optional<RawChange>> fut) {
            if (rs.getAvailableWithoutFetching() == 0) {
                if (rs.isFullyFetched()) {
                    fut.complete(Optional.empty());
                } else {
                    shouldTryRecreateSchema = true;
                    Futures.addCallback(rs.fetchMoreResults(), new FutureCallback<ResultSet>() {

                        @Override
                        public void onSuccess(ResultSet result) {
                            // There's no guarantee what thread will run this
                            rs = result;
                            findNext(fut);
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            fut.completeExceptionally(t);
                        }
                    }, MoreExecutors.directExecutor());
                }
            } else {
                // Since 3.11.5.9 java driver
                // which has both metadata id extension
                // and mitigations that prevent setting skipMetadata
                // for ambiguous queries, the schema of the rows may
                // change between pages, invalidating the first
                // assumption that was here previously.
                //
                // The second assumption is that
                // between a time that the CDC log query
                // was prepared (and therefore a CDC schema was
                // returned by the preparing of the query)
                // and fetching of the base table schema (now)
                // there was at most a single schema change.
                Row row = rs.one();
                if (shouldTryRecreateSchema) {
                    try {
                        int newHashcode = row.getColumnDefinitions().hashCode();
                        if (newHashcode != lastPageColDefsHashcode || schema == null) {
                            schema = Driver3SchemaFactory.getChangeSchema(row, session.getCluster().getMetadata());
                            lastPageColDefsHashcode = newHashcode;
                        }
                        shouldTryRecreateSchema = false;
                    } catch (Driver3SchemaFactory.UnresolvableSchemaInconsistencyException ex) {
                        fut.completeExceptionally(ex);
                        return;
                    }
                }
                Driver3RawChange newChange = new Driver3RawChange(row, schema);

                // lastChangeId determines the point from which we should
                // start reading within a window. In this implementation
                // we simply read the entire window and skip rows that
                // were before lastChangeId.
                //
                // If lastChangeId is Optional.empty(), then we read
                // the entire window.
                if (!lastChangeId.isPresent() || newChange.getId().compareTo(lastChangeId.get()) > 0) {
                    fut.complete(Optional.of(newChange));
                } else {
                    findNext(fut);
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

    protected class Driver3MultiReader implements Reader {

        protected volatile List<Driver3Reader> readers;
        protected AtomicInteger currentReaderIndex;

        public Driver3MultiReader(List<ResultSet> rss, Optional<ChangeId> lastChangeId) {
            this.readers = rss.stream().map(rs -> new Driver3Reader(rs, lastChangeId)).collect(Collectors.toList());
            this.currentReaderIndex = new AtomicInteger();
        }

        private void findNext(CompletableFuture<Optional<RawChange>> fut) {
            // use many readers
            if(currentReaderIndex.get() >= readers.size()) {
                fut.complete(Optional.empty());
            }

            readers.get(currentReaderIndex.get()).nextChange()
                .whenCompleteAsync((change, exception) -> {
                    if (exception != null) {
                        fut.completeExceptionally(exception);
                    } else {
                        if(!change.isPresent()) {
                            currentReaderIndex.incrementAndGet();
                            findNext(fut);
                        } else {
                            fut.complete(change);
                        }
                    }
                });
        }

        @Override
        public CompletableFuture<Optional<RawChange>> nextChange() {
            CompletableFuture<Optional<RawChange>> result = new CompletableFuture<>();
            findNext(result);
            return result;
        }

    }

    protected List<ResultSetFuture> queryTables(PreparedStatement stmt, Task task) {
        List<ResultSetFuture> futures = task.streams.stream().map(StreamId::getValue)
            .map(streamId ->
                session.executeAsync(
                    stmt.bind(
                        streamId, task.state.getWindowStart(), task.state.getWindowEnd()
                    )
                    .setConsistencyLevel(consistencyLevel)
                )
            ).collect(Collectors.toList());
        logger.atFine().log("Querying window: [%s, %s] for task: %s, task state: %s", task.state.getWindowStart(), task.state.getWindowEnd(), task.id, task.state);
        return futures;
    }

    protected CompletableFuture<Reader> wrapResults(Task task, List<ResultSetFuture> futures) {
        CompletableFuture<Reader> result = new CompletableFuture<>();
        Futures.addCallback(Futures.allAsList(futures), new FutureCallback<List<ResultSet>>() {
            @Override
            public void onSuccess(List<ResultSet> rss) {
                result.complete(new Driver3MultiReader(rss, task.state.getLastConsumedChangeId()));
            }

            @Override
            public void onFailure(Throwable t) {
                result.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
        return result;
    }

    protected CompletableFuture<Reader> query(PreparedStatement stmt, Task task) {
        List<ResultSetFuture> futures = queryTables(stmt, task);
        return wrapResults(task, futures);
    }

    @Override
    public CompletableFuture<Reader> createReader(Task task) {
        PreparedStatement stmt = preparedStmts.get(task.id.getTable());
        Preconditions.checkNotNull(stmt);
        return query(stmt, task);
    }

    @Override
    public CompletableFuture<Optional<Long>> fetchTableTTL(TableName tableName) {
        return Driver3CommonCQL.fetchTableTTL(session, tableName);
    }

    private static RegularStatement getProbeStmt(TableName table) {
        return select().column(quoteIfNecessary("cdc$time"))
                .from(quoteIfNecessary(table.keyspace), quoteIfNecessary(table.name + "_scylla_cdc_log"))
                .where(eq(quoteIfNecessary("cdc$stream_id"), bindMarker()))
                .and(gt(quoteIfNecessary("cdc$time"), bindMarker()))
                .orderBy(QueryBuilder.asc(quoteIfNecessary("cdc$time")))
                .limit(1);
    }

    private CompletableFuture<PreparedStatement> prepareProbe(TableName table) {
        PreparedStatement existing = probeStmts.get(table);
        if (existing != null) {
            return CompletableFuture.completedFuture(existing);
        }
        return probeFutures.computeIfAbsent(table, tbl -> {
            CompletableFuture<PreparedStatement> result = new CompletableFuture<>();
            Futures.addCallback(session.prepareAsync(getProbeStmt(tbl)), new FutureCallback<PreparedStatement>() {
                @Override
                public void onSuccess(PreparedStatement r) {
                    probeStmts.putIfAbsent(tbl, r);
                    result.complete(probeStmts.get(tbl));
                }
                @Override
                public void onFailure(Throwable ex) {
                    result.completeExceptionally(ex);
                }
            }, MoreExecutors.directExecutor());
            return result;
        });
    }

    @Override
    public CompletableFuture<Optional<Timestamp>> fetchFirstChangeTime(TableName table, StreamId streamId, Timestamp after, long readTimeoutMs) {
        return prepareProbe(table).thenCompose(stmt -> {
            CompletableFuture<Optional<Timestamp>> result = new CompletableFuture<>();
            // UUIDs.startOf() creates the smallest possible timeuuid for the given
            // millisecond. This is correct for our "cdc$time > ?" query because any
            // real CDC timeuuid at the same millisecond will have a larger clock_seq/node
            // component and thus compare greater.
            //
            // setFetchSize(1) prevents the driver from fetching more rows than needed.
            // On streams with only old, TTL-expired data, this query may still scan
            // tombstones. Ensure gc_grace_seconds is configured appropriately on CDC
            // log tables to minimize this cost.
            // Use LOCAL_ONE for probe queries since they are best-effort:
            // if the probe returns stale data the worker simply reads from
            // an earlier window, which is safe.
            // Per-query timeout bounds individual probe latency to prevent
            // slow scans on streams with many tombstones from blocking other probes.
            ResultSetFuture future = session.executeAsync(
                    stmt.bind(streamId.getValue(), com.datastax.driver.core.utils.UUIDs.startOf(after.toDate().getTime()))
                            .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
                            .setReadTimeoutMillis((int) Math.min(readTimeoutMs, Integer.MAX_VALUE))
                            .setFetchSize(1));
            Futures.addCallback(future, new FutureCallback<ResultSet>() {
                @Override
                public void onSuccess(ResultSet rs) {
                    Row row = rs.one();
                    if (row != null) {
                        // UUIDs.unixTimestamp() extracts millisecond-precision from the
                        // 100ns-precision timeuuid. This means the window start after
                        // catch-up may be up to ~1ms earlier than the actual first change,
                        // potentially causing a single redundant re-read — acceptable.
                        result.complete(Optional.of(new Timestamp(new Date(
                                com.datastax.driver.core.utils.UUIDs.unixTimestamp(row.getUUID(0))))));
                    } else {
                        result.complete(Optional.empty());
                    }
                }
                @Override
                public void onFailure(Throwable t) {
                    result.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
            return result;
        });
    }
}
