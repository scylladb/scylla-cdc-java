package com.scylladb.cdc.cql;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.master.GenerationMetadata;
import com.scylladb.cdc.model.TableName;

public abstract class BaseMasterCQL implements MasterCQL {

    protected abstract CompletableFuture<Optional<Date>> fetchSmallestGenerationAfter(Date after);

    protected abstract CompletableFuture<Set<ByteBuffer>> fetchStreamsForGeneration(Date generationStart);

    protected abstract CompletableFuture<Optional<Date>> fetchSmallestTableGenerationAfter(TableName table, Date after);

    protected abstract CompletableFuture<Set<ByteBuffer>> fetchStreamsForTableGeneration(TableName table, Date generationStart);

    protected abstract CompletableFuture<Optional<Date>> fetchLargestGenerationBeforeOrAt(Date cutoff);

    protected abstract CompletableFuture<Optional<Date>> fetchLargestTableGenerationBeforeOrAt(TableName table, Date cutoff);

    @Override
    public CompletableFuture<Optional<GenerationId>> fetchFirstGenerationId() {
        return fetchFirstGenerationIdAfter(new Date(0));
    }

    @Override
    public CompletableFuture<Optional<GenerationId>> fetchLastGenerationBeforeOrAt(Date cutoff) {
        return fetchLargestGenerationBeforeOrAt(cutoff)
                .thenApply(opt -> opt.map(t -> new GenerationId(new Timestamp(t))));
    }

    @Override
    public CompletableFuture<Optional<GenerationId>> fetchLastTableGenerationBeforeOrAt(TableName table, Date cutoff) {
        return fetchLargestTableGenerationBeforeOrAt(table, cutoff)
                .thenApply(opt -> opt.map(t -> new GenerationId(new Timestamp(t))));
    }

    @Override
    public CompletableFuture<Optional<GenerationId>> fetchFirstGenerationIdAfter(Date after) {
        return fetchSmallestGenerationAfter(after)
                .thenApply(opt -> opt.map(t -> new GenerationId(new Timestamp(t))));
    }

    private static SortedSet<StreamId> convertStreams(Set<ByteBuffer> input) {
        return input.stream().map(StreamId::new).collect(Collectors.toCollection(() -> new TreeSet<>()));
    }

    @Override
    public CompletableFuture<GenerationMetadata> fetchGenerationMetadata(GenerationId id) {
        CompletableFuture<Optional<Timestamp>> endFut = fetchGenerationEnd(id);
        CompletableFuture<Set<ByteBuffer>> streamsFut = fetchStreamsForGeneration(id.getGenerationStart().toDate());
        return endFut.thenCombine(streamsFut,
                (end, streams) -> new GenerationMetadata(id.getGenerationStart(), end, convertStreams(streams)));
    }

    @Override
    public CompletableFuture<Optional<Timestamp>> fetchGenerationEnd(GenerationId id) {
        return fetchSmallestGenerationAfter(id.getGenerationStart().toDate()).thenApply(opt -> opt.map(Timestamp::new));
    }

    @Override
    public CompletableFuture<GenerationId> fetchFirstTableGenerationId(TableName table) {
        return fetchFirstTableGenerationIdAfter(table, new Date(0));
    }

    @Override
    public CompletableFuture<GenerationId> fetchFirstTableGenerationIdAfter(TableName table, Date after) {
        return fetchSmallestTableGenerationAfter(table, after)
                .thenApply(opt -> opt.map(t -> new GenerationId(new Timestamp(t)))
                .orElseThrow(() -> new IllegalStateException("No generation found for table: " + table + " after: " + after)));
    }

    @Override
    public CompletableFuture<GenerationMetadata> fetchTableGenerationMetadata(TableName table, GenerationId generationId) {
        CompletableFuture<Optional<Timestamp>> endFut = fetchTableGenerationEnd(table, generationId);
        CompletableFuture<Set<ByteBuffer>> streamsFut = fetchStreamsForTableGeneration(table, generationId.getGenerationStart().toDate());

        return endFut.thenCombine(streamsFut, (end, streams) -> {
            return new GenerationMetadata(generationId.getGenerationStart(), end, convertStreams(streams));
        });
    }

    @Override
    public CompletableFuture<Optional<Timestamp>> fetchTableGenerationEnd(TableName table, GenerationId generationId) {
        return fetchSmallestTableGenerationAfter(table, generationId.getGenerationStart().toDate())
                .thenApply(opt -> opt.map(t -> new Timestamp(t)));
    }

}
