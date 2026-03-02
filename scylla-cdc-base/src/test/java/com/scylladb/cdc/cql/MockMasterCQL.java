package com.scylladb.cdc.cql;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.master.GenerationMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class MockMasterCQL implements MasterCQL {
    private volatile List<GenerationMetadata> generationMetadatas;
    private volatile Map<TableName, List<GenerationMetadata>> tableGenerationMetadatas = new HashMap<>();
    private volatile Map<TableName, Optional<Long>> tablesTTL = new HashMap<>();
    private volatile boolean shouldInjectFailure = false;
    private volatile boolean shouldInjectCatchUpFailure = false;
    private volatile boolean usesTablets = false;
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

    public void setTableGenerationMetadatas(Map<TableName, List<GenerationMetadata>> tableGenerationMetadatas) {
        this.tableGenerationMetadatas = tableGenerationMetadatas;
    }

    public void setShouldInjectCatchUpFailure(boolean injectCatchUpFailure) {
        this.shouldInjectCatchUpFailure = injectCatchUpFailure;
    }

    public void setUsesTablets(boolean usesTablets) {
        this.usesTablets = usesTablets;
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
    public CompletableFuture<Optional<GenerationId>> fetchFirstGenerationIdAfter(Date after) {
        if (shouldInjectFailure) {
            return injectFailure();
        } else {
            successfulFetchCount.incrementAndGet();
        }

        Optional<GenerationId> result = generationMetadatas.stream()
                .map(GenerationMetadata::getId)
                .filter(id -> id.getGenerationStart().toDate().after(after))
                .min(Comparator.naturalOrder());
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public CompletableFuture<Optional<GenerationId>> fetchLastGenerationBeforeOrAt(Date cutoff) {
        if (shouldInjectFailure || shouldInjectCatchUpFailure) {
            return injectFailure();
        } else {
            successfulFetchCount.incrementAndGet();
        }

        Optional<GenerationId> result = generationMetadatas.stream()
                .map(GenerationMetadata::getId)
                .filter(id -> !id.getGenerationStart().toDate().after(cutoff))
                .max(Comparator.naturalOrder());
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public CompletableFuture<Optional<GenerationId>> fetchLastTableGenerationBeforeOrAt(TableName table, Date cutoff) {
        if (shouldInjectFailure || shouldInjectCatchUpFailure) {
            return injectFailure();
        } else {
            successfulFetchCount.incrementAndGet();
        }

        List<GenerationMetadata> metas = tableGenerationMetadatas.getOrDefault(table, new ArrayList<>());
        Optional<GenerationId> result = metas.stream()
                .map(GenerationMetadata::getId)
                .filter(id -> !id.getGenerationStart().toDate().after(cutoff))
                .max(Comparator.naturalOrder());
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public CompletableFuture<GenerationId> fetchFirstTableGenerationIdAfter(TableName table, Date after) {
        if (shouldInjectFailure) {
            return injectFailure();
        } else {
            successfulFetchCount.incrementAndGet();
        }

        List<GenerationMetadata> metas = tableGenerationMetadatas.getOrDefault(table, new ArrayList<>());
        Optional<GenerationId> result = metas.stream()
                .map(GenerationMetadata::getId)
                .filter(id -> id.getGenerationStart().toDate().after(after))
                .min(Comparator.naturalOrder());
        if (!result.isPresent()) {
            CompletableFuture<GenerationId> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(new IllegalArgumentException(
                    "No table generation metadata for table: " + table + " after: " + after));
            return failedFuture;
        }
        return CompletableFuture.completedFuture(result.get());
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

    @Override
    public CompletableFuture<Optional<Throwable>> validateTable(TableName table) {
        if (shouldInjectFailure) {
            return injectFailure();
        } else {
            successfulFetchCount.incrementAndGet();
        }

        // All tables are correct. (Present and with CDC enabled).
        return CompletableFuture.completedFuture(Optional.empty());
    }

    private <T> CompletableFuture<T> injectFailure() {
        failedFetchCount.incrementAndGet();

        String calleeName = Thread.currentThread().getStackTrace()[2].getMethodName();
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(new IllegalAccessError(String.format("Injected %s() fail.", calleeName)));
        return result;
    }

    @Override
    public CompletableFuture<GenerationId> fetchFirstTableGenerationId(TableName table) {
        if (shouldInjectFailure) {
            return injectFailure();
        } else {
            successfulFetchCount.incrementAndGet();
        }

        List<GenerationMetadata> metas = tableGenerationMetadatas.getOrDefault(table, new ArrayList<>());
        if (metas.isEmpty()) {
            CompletableFuture<GenerationId> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(new IllegalArgumentException("No table generation metadata for table: " + table));
            return failedFuture;
        }

        return CompletableFuture.completedFuture(metas.get(0).getId());
    }

    @Override
    public CompletableFuture<GenerationMetadata> fetchTableGenerationMetadata(TableName table, GenerationId generationId) {
        if (shouldInjectFailure) {
            return injectFailure();
        } else {
            successfulFetchCount.incrementAndGet();
        }

        List<GenerationMetadata> metas = tableGenerationMetadatas.getOrDefault(table, new ArrayList<>());
        for (GenerationMetadata meta : metas) {
            if (meta.getId().equals(generationId)) {
                return CompletableFuture.completedFuture(meta);
            }
        }
        CompletableFuture<GenerationMetadata> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new IllegalArgumentException(
            String.format("Could not fetch table generation metadata with id: %s for table: %s", generationId, table)));
        return failedFuture;
    }

    @Override
    public CompletableFuture<Optional<Timestamp>> fetchTableGenerationEnd(TableName table, GenerationId generationId) {
        if (shouldInjectFailure) {
            return injectFailure();
        } else {
            successfulFetchCount.incrementAndGet();
        }

        // Find the metadata for this generation
        List<GenerationMetadata> metas = tableGenerationMetadatas.getOrDefault(table, new ArrayList<>());
        for (GenerationMetadata meta : metas) {
            if (meta.getId().equals(generationId)) {
                return CompletableFuture.completedFuture(meta.getEnd());
            }
        }

        // If generation not found, return empty
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public Boolean usesTablets(TableName tableName) {
        return usesTablets;
    }
}
