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
}
