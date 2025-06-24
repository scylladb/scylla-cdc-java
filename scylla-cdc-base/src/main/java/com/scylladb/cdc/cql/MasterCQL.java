package com.scylladb.cdc.cql;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.master.GenerationMetadata;

public interface MasterCQL {

    CompletableFuture<Optional<Long>> fetchTableTTL(TableName tableName);
    CompletableFuture<Optional<Throwable>> validateTable(TableName table);

    // Vnode-based CDC methods
    CompletableFuture<Optional<GenerationId>> fetchFirstGenerationId();
    CompletableFuture<GenerationMetadata> fetchGenerationMetadata(GenerationId id);
    CompletableFuture<Optional<Timestamp>> fetchGenerationEnd(GenerationId id);

    // Tablet-based CDC methods
    CompletableFuture<GenerationId> fetchFirstTableGenerationId(TableName table);
    CompletableFuture<GenerationMetadata> fetchTableGenerationMetadata(TableName table, GenerationId generationId);
    CompletableFuture<Optional<Timestamp>> fetchTableGenerationEnd(TableName table, GenerationId generationId);
}
