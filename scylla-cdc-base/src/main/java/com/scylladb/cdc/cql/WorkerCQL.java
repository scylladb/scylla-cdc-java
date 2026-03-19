package com.scylladb.cdc.cql;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.Task;

public interface WorkerCQL {
    public static interface Reader {
        CompletableFuture<Optional<RawChange>> nextChange();
    }

    void prepare(Set<TableName> tables) throws InterruptedException, ExecutionException;

    CompletableFuture<Reader> createReader(Task task);

    CompletableFuture<Optional<Long>> fetchTableTTL(TableName tableName);

    /**
     * Fetches the timestamp of the first CDC change in the given stream
     * that occurred strictly after the specified timestamp.
     *
     * <p>Used by the catch-up optimization to probe whether a stream has
     * any data and, if so, where it starts. This allows the worker to skip
     * empty time windows.
     *
     * <p><strong>Performance note:</strong> The underlying query
     * ({@code SELECT ... LIMIT 1}) on streams with no data after the given
     * timestamp may scan large partitions containing expired tombstones.
     * This is a known trade-off: the probe is cheap when data exists nearby,
     * but may be slow on streams that have only old, TTL-expired data.
     *
     * @param table the base table name (not the CDC log table)
     * @param streamId the CDC stream to probe
     * @param after exclusive lower bound — only changes after this timestamp are considered.
     *              Implementations typically use {@code UUIDs.startOf(after.toDate().getTime())}
     *              to create the smallest timeuuid for the given millisecond, then query with
     *              {@code cdc$time > ?}. Since real CDC timeuuids are always larger than
     *              {@code startOf} at the same millisecond (they have non-zero clock_seq/node
     *              components), this effectively finds changes at or after that millisecond.
     * @param readTimeoutMs per-query read timeout in milliseconds; bounds individual probe
     *                      latency to prevent slow scans from blocking other probes.
     *                      Note: this controls the <em>client-side</em> timeout. The server-side
     *                      {@code read_request_timeout_in_ms} may cause the coordinator to fail
     *                      the request independently. On heavily loaded clusters, even
     *                      {@code LIMIT 1} queries can be slow due to tombstone scanning.
     *                      Ensure {@code gc_grace_seconds} is configured appropriately on
     *                      CDC log tables to minimize this cost.
     * @return a future completing with the timestamp of the first change,
     *         or {@link Optional#empty()} if no changes exist after the given timestamp.
     *         Implementations should use a low consistency level (e.g. ONE) for performance.
     */
    default CompletableFuture<Optional<Timestamp>> fetchFirstChangeTime(TableName table, StreamId streamId, Timestamp after, long readTimeoutMs) {
        throw new UnsupportedOperationException("fetchFirstChangeTime is not implemented; "
                + "required for catch-up optimization (withCatchUpWindowSizeSeconds > 0)");
    }
}
