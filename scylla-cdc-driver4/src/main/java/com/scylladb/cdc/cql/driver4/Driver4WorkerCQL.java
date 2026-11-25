package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.connection.BusyConnectionException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.Task;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Reads the CDC log of a task window with Java driver 4.x, the counterpart of
 * {@code Driver3WorkerCQL}.
 */
public class Driver4WorkerCQL implements WorkerCQL {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    // BusyConnectionException replaces 3.x BusyPoolException: 4.x has no per-host request queue.
    private static final ImmutableSet<Class<? extends Throwable>> NOISY_EXCEPTIONS =
            ImmutableSet.of(BusyConnectionException.class, OverloadedException.class, ReadTimeoutException.class);

    private final CqlSession session;
    // Concurrent: tablet mode prepares later tables while running tasks look up earlier ones.
    private final Map<TableName, PreparedStatement> preparedStmts = new ConcurrentHashMap<>();
    private final ConsistencyLevel consistencyLevel;

    public Driver4WorkerCQL(Driver4Session session) {
        this.session = Preconditions.checkNotNull(session).getDriverSession();
        this.consistencyLevel = session.getConsistencyLevel();
    }

    /**
     * Returns the query 3.x builds, quoting names as its {@code Metadata.quoteIfNecessary} does.
     */
    @VisibleForTesting
    static String getStmt(TableName table) {
        return "SELECT * FROM " + quoteIfNecessary(table.keyspace) + "." + quoteIfNecessary(table.name + "_scylla_cdc_log")
                + " WHERE " + quoteIfNecessary("cdc$stream_id") + "=? AND " + quoteIfNecessary("cdc$time") + ">? AND "
                + quoteIfNecessary("cdc$time") + "<=?;";
    }

    private static String quoteIfNecessary(String name) {
        return CqlIdentifier.fromInternal(name).asCql(true);
    }

    private CompletableFuture<Map.Entry<TableName, PreparedStatement>> prepare(TableName table) {
        return session.prepareAsync(getStmt(table)).toCompletableFuture()
                .thenApply(stmt -> Map.entry(table, stmt));
    }

    @Override
    public void prepare(Set<TableName> tables) throws InterruptedException, ExecutionException {
        @SuppressWarnings("unchecked")
        CompletableFuture<Map.Entry<TableName, PreparedStatement>>[] futures = tables.stream()
                .filter(t -> !preparedStmts.containsKey(t)).map(this::prepare).toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).get();
        for (CompletableFuture<Map.Entry<TableName, PreparedStatement>> f : futures) {
            Map.Entry<TableName, PreparedStatement> r = f.get();
            preparedStmts.put(r.getKey(), r.getValue());
        }
    }

    protected class Driver4Reader implements Reader {
        protected volatile AsyncResultSet rs;
        private volatile ChangeSchema schema;
        protected final Optional<ChangeId> lastChangeId;
        private volatile boolean shouldTryRecreateSchema = true;
        private volatile ColumnDefinitions lastPageColumnDefinitions;

        public Driver4Reader(AsyncResultSet rs, Optional<ChangeId> lastChangeId) {
            this.rs = Preconditions.checkNotNull(rs);
            this.lastChangeId = Preconditions.checkNotNull(lastChangeId);
        }

        protected void findNext(CompletableFuture<Optional<RawChange>> fut) {
            if (rs.remaining() == 0) {
                if (!rs.hasMorePages()) {
                    fut.complete(Optional.empty());
                } else {
                    shouldTryRecreateSchema = true;
                    rs.fetchNextPage().whenComplete((result, t) -> {
                        if (t != null) {
                            fut.completeExceptionally(t);
                        } else {
                            // There's no guarantee what thread will run this
                            rs = result;
                            findNext(fut);
                        }
                    });
                }
            } else {
                // The schema of the rows may change between pages. Between preparing the CDC log
                // query and reading the base table schema now, there was at most one schema change.
                Row row = rs.one();
                if (shouldTryRecreateSchema) {
                    try {
                        // 4.x ColumnDefinitions has no equals: pages share one instance until a
                        // schema change, which brings a new one.
                        ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
                        if (columnDefinitions != lastPageColumnDefinitions || schema == null) {
                            schema = Driver4SchemaFactory.getChangeSchema(row, session.getMetadata());
                            lastPageColumnDefinitions = columnDefinitions;
                        }
                        shouldTryRecreateSchema = false;
                    } catch (Driver4SchemaFactory.UnresolvableSchemaInconsistencyException ex) {
                        fut.completeExceptionally(ex);
                        return;
                    }
                }
                Driver4RawChange newChange = new Driver4RawChange(row, schema);

                // Read the entire window and skip the rows up to lastChangeId, if any.
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

    protected class Driver4MultiReader implements Reader {
        protected volatile List<Driver4Reader> readers;
        protected AtomicInteger currentReaderIndex;

        public Driver4MultiReader(List<AsyncResultSet> rss, Optional<ChangeId> lastChangeId) {
            this.readers = rss.stream().map(rs -> new Driver4Reader(rs, lastChangeId)).collect(Collectors.toList());
            this.currentReaderIndex = new AtomicInteger();
        }

        private void findNext(CompletableFuture<Optional<RawChange>> fut) {
            if (currentReaderIndex.get() >= readers.size()) {
                fut.complete(Optional.empty());
                // 3.x falls through here and fails on the missing reader.
                return;
            }

            readers.get(currentReaderIndex.get()).nextChange()
                    .whenCompleteAsync((change, exception) -> {
                        if (exception != null) {
                            fut.completeExceptionally(exception);
                        } else if (!change.isPresent()) {
                            currentReaderIndex.incrementAndGet();
                            findNext(fut);
                        } else {
                            fut.complete(change);
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

    protected List<CompletableFuture<AsyncResultSet>> queryTables(PreparedStatement stmt, Task task) {
        List<CompletableFuture<AsyncResultSet>> futures = task.streams.stream().map(StreamId::getValue)
                .map(streamId -> session.executeAsync(
                        stmt.bind(streamId, task.state.getWindowStart(), task.state.getWindowEnd())
                                .setConsistencyLevel(consistencyLevel)
                                // A SELECT, as 3.x QueryBuilder marks it.
                                .setIdempotent(true)).toCompletableFuture())
                .collect(Collectors.toList());
        logger.atFine().log("Querying window: [%s, %s] for task: %s, task state: %s",
                task.state.getWindowStart(), task.state.getWindowEnd(), task.id, task.state);
        return futures;
    }

    protected CompletableFuture<Reader> wrapResults(Task task, List<CompletableFuture<AsyncResultSet>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).thenApply(ignored -> {
            List<AsyncResultSet> rss = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
            Optional<ChangeId> lastChangeId = task.state.getLastConsumedChangeId();
            if (task.id.isTabletStreamTask() && rss.size() == 1) {
                // Per-stream tablet tasks have exactly one result set. Avoid routing every row
                // through Driver4MultiReader's common-pool continuation in this hot path.
                return new Driver4Reader(rss.get(0), lastChangeId);
            }
            return new Driver4MultiReader(rss, lastChangeId);
        });
    }

    protected CompletableFuture<Reader> query(PreparedStatement stmt, Task task) {
        return wrapResults(task, queryTables(stmt, task));
    }

    @Override
    public CompletableFuture<Reader> createReader(Task task) {
        PreparedStatement stmt = preparedStmts.get(task.id.getTable());
        Preconditions.checkNotNull(stmt);
        return query(stmt, task);
    }

    @Override
    public CompletableFuture<Optional<Long>> fetchTableTTL(TableName tableName) {
        return Driver4CommonCQL.fetchTableTTL(session, tableName);
    }

    private static boolean isDirectNoisyException(Throwable ex) {
        return NOISY_EXCEPTIONS.stream().anyMatch(c -> c.isInstance(ex));
    }

    @Override
    public boolean isNoisyException(Throwable ex) {
        return isNoisyDriverFailure(ex);
    }

    static boolean isNoisyDriverFailure(Throwable ex) {
        if (isDirectNoisyException(ex)) {
            return true;
        }
        if (ex instanceof AllNodesFailedException) {
            // Every attempt on every node must be noisy; NoNodeAvailableException has none.
            Collection<List<Throwable>> errors = ((AllNodesFailedException) ex).getAllErrors().values();
            return !errors.isEmpty() && errors.stream().flatMap(List::stream).allMatch(Driver4WorkerCQL::isDirectNoisyException);
        }
        return false;
    }
}
