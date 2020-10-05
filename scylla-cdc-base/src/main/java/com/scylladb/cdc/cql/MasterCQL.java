package com.scylladb.cdc.cql;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.master.GenerationMetadata;

public interface MasterCQL {
    CompletableFuture<Optional<GenerationId>> fetchFirstGenerationId();
    CompletableFuture<GenerationMetadata> fetchGenerationMetadata(GenerationId id);
    CompletableFuture<Optional<Timestamp>> fetchGenerationEnd(GenerationId id);
}
