package com.scylladb.cdc.cql;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.scylladb.cdc.model.TableName;
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
     * Whether a failure is expected to recur during a transient CQL problem and
     * may have its repeated worker log messages suppressed. Implementations that
     * do not classify failures keep the existing logging behavior.
     */
    default boolean isNoisyException(Throwable exception) {
        return false;
    }
}
