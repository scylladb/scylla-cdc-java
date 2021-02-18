package com.scylladb.cdc.cql;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.master.GenerationMetadata;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class MockMasterCQL implements MasterCQL {
    private volatile List<GenerationMetadata> generationMetadatas;
    private volatile Map<TableName, Optional<Long>> tablesTTL = new HashMap<>();
    private volatile boolean shouldInjectFailure = false;
    private final AtomicInteger failedFetchCount = new AtomicInteger(0);
    private final AtomicInteger successfulFetchCount = new AtomicInteger(0);

    public MockMasterCQL() {
        this(Collections.emptyList());
    }

    public MockMasterCQL(List<GenerationMetadata> generationMetadatas) {
        this.generationMetadatas = Preconditions.checkNotNull(generationMetadatas);
    }

    public void setGenerationMetadatas(List<GenerationMetadata> generationMetadatas) {
        this.generationMetadatas = Preconditions.checkNotNull(generationMetadatas);
    }

    public void setTablesTTL(Map<TableName, Optional<Long>> tablesTTL) {
        this.tablesTTL = tablesTTL;
    }

    public void setShouldInjectFailure(boolean injectFailure) {
        this.shouldInjectFailure = injectFailure;
    }

    public int getFailedFetchCount() {
        return failedFetchCount.get();
    }

    public int getSuccessfulFetchCount() {
        return successfulFetchCount.get();
    }

    public int getFetchCount() {
        return getFailedFetchCount() + getSuccessfulFetchCount();
    }

    @Override
    public CompletableFuture<Optional<GenerationId>> fetchFirstGenerationId() {
        if (shouldInjectFailure) {
            return injectFailure();
        } else {
            successfulFetchCount.incrementAndGet();
        }

        Optional<GenerationId> result = generationMetadatas.stream().map(GenerationMetadata::getId).min(Comparator.naturalOrder());
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public CompletableFuture<GenerationMetadata> fetchGenerationMetadata(GenerationId id) {
        if (shouldInjectFailure) {
            return injectFailure();
        } else {
            successfulFetchCount.incrementAndGet();
        }

        Optional<GenerationMetadata> generationMetadata = generationMetadatas.stream().filter(g -> g.getId().equals(id)).findFirst();
        return generationMetadata.map(CompletableFuture::completedFuture).orElseGet(() -> {
            CompletableFuture<GenerationMetadata> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(new IllegalArgumentException(
                    String.format("Could not fetch generation metadata with id: %s", id)));
            return failedFuture;
        });
    }

    @Override
    public CompletableFuture<Optional<Timestamp>> fetchGenerationEnd(GenerationId id) {
        if (shouldInjectFailure) {
            return injectFailure();
        }

        // No successfulFetchCount increment, because we call
        // fetchGenerationMetadata which increments it itself.

        return fetchGenerationMetadata(id).thenApply(GenerationMetadata::getEnd);
    }

    @Override
    public CompletableFuture<Optional<Long>> fetchTableTTL(TableName tableName) {
        if (shouldInjectFailure) {
            return injectFailure();
        } else {
            successfulFetchCount.incrementAndGet();
        }

        Optional<Long> ttl = tablesTTL.getOrDefault(tableName, Optional.empty());
        return CompletableFuture.completedFuture(ttl);
    }

    private <T> CompletableFuture<T> injectFailure() {
        failedFetchCount.incrementAndGet();

        String calleeName = Thread.currentThread().getStackTrace()[2].getMethodName();
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(new IllegalAccessError(String.format("Injected %s() fail.", calleeName)));
        return result;
    }
}
