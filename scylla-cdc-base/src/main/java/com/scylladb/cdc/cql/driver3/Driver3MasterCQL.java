package com.scylladb.cdc.cql.driver3;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.column;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.scylladb.cdc.cql.BaseMasterCQL;

public final class Driver3MasterCQL extends BaseMasterCQL {

    private final Session session;
    private final PreparedStatement fetchSmallestGenerationAfterStmt;
    private final PreparedStatement fetchStreamsStmt;

    public Driver3MasterCQL(Session session) {
        Preconditions.checkNotNull(session);
        this.session = session;
        fetchSmallestGenerationAfterStmt = session.prepare(getFetchSmallestGenerationAfter());
        fetchStreamsStmt = session.prepare(getFetchStreams());
    }

    private static RegularStatement getFetchSmallestGenerationAfter() {
        return select().min(column("time")).from("system_distributed", "cdc_streams_descriptions")
                .where(gt("time", bindMarker())).allowFiltering();
    }

    private static RegularStatement getFetchStreams() {
        return select().column("streams").from("system_distributed", "cdc_streams_descriptions")
                .where(eq("time", bindMarker())).allowFiltering();
    }

    private ConsistencyLevel computeCL() {
        return session.getCluster().getMetadata().getAllHosts().size() > 1 ? ConsistencyLevel.QUORUM
                : ConsistencyLevel.ONE;
    }

    private void consumeResult(ResultSet rs, CompletableFuture<Optional<Row>> result) {
        int availCount = rs.getAvailableWithoutFetching();
        if (availCount == 0) {
            if (rs.isFullyFetched()) {
                result.complete(Optional.empty());
            } else {
                Futures.addCallback(rs.fetchMoreResults(), new FutureCallback<ResultSet>() {

                    @Override
                    public void onSuccess(ResultSet rs) {
                        consumeResult(rs, result);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        result.completeExceptionally(t);
                    }
                });
            }
        } else {
            assert (availCount == 1);
            result.complete(Optional.of(rs.one()));
        }
    }

    private CompletableFuture<Optional<Row>> execute(BoundStatement stmt) {
        CompletableFuture<Optional<Row>> result = new CompletableFuture<>();
        ResultSetFuture future = session.executeAsync(stmt.setConsistencyLevel(computeCL()));
        Futures.addCallback(future, new FutureCallback<ResultSet>() {

            @Override
            public void onSuccess(ResultSet rs) {
                consumeResult(rs, result);
            }

            @Override
            public void onFailure(Throwable t) {
                result.completeExceptionally(t);
            }
        });
        return result;
    }

    @Override
    protected CompletableFuture<Optional<Date>> fetchSmallestGenerationAfter(Date after) {
        return execute(fetchSmallestGenerationAfterStmt.bind(after)).thenApply(o -> o.map(r -> r.getTimestamp(0)));
    }

    @Override
    protected CompletableFuture<Set<ByteBuffer>> fetchStreamsForGeneration(Date generationStart) {
        return execute(fetchStreamsStmt.bind(generationStart)).thenApply(o -> o.get().getSet(0, ByteBuffer.class));
    }

}
