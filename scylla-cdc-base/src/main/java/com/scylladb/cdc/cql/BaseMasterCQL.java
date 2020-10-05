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

public abstract class BaseMasterCQL implements MasterCQL {

    protected abstract CompletableFuture<Optional<Date>> fetchSmallestGenerationAfter(Date after);

    protected abstract CompletableFuture<Set<ByteBuffer>> fetchStreamsForGeneration(Date generationStart);

    @Override
    public CompletableFuture<Optional<GenerationId>> fetchFirstGenerationId() {
        return fetchSmallestGenerationAfter(new Date(0))
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

}
