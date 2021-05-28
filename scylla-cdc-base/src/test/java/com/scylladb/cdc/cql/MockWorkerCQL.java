package com.scylladb.cdc.cql;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.error.worker.NoOpErrorInject;
import com.scylladb.cdc.cql.error.worker.ErrorInject;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MockWorkerCQL implements WorkerCQL {
    private volatile Map<TableName, Optional<Long>> tablesTTL = new HashMap<>();
    private volatile List<RawChange> rawChanges = Collections.emptyList();
    private final Set<Task> createReaderInvocations = ConcurrentHashMap.newKeySet();
    private final Set<Task> finishedReaders = ConcurrentHashMap.newKeySet();
    private volatile ErrorInject cqlErrorStrategy = new NoOpErrorInject();
    private final AtomicInteger failureCount = new AtomicInteger(0);

    class MockReaderCQL implements WorkerCQL.Reader {
        private final Task task;
        private final Iterator<RawChange> rawChangeIterator;

        public MockReaderCQL(Task task, Iterator<RawChange> rawChangeIterator) {
            this.task = task;
            this.rawChangeIterator = Preconditions.checkNotNull(rawChangeIterator);
        }

        @Override
        public CompletableFuture<Optional<RawChange>> nextChange() {
            if (rawChangeIterator.hasNext()) {
                Optional<RawChange> readChange = Optional.of(rawChangeIterator.next());

                // Maybe inject an error.
                CompletableFuture<Optional<RawChange>> injectedError = cqlErrorStrategy.injectFailure(readChange);
                if (injectedError != null) {
                    failureCount.incrementAndGet();
                    return injectedError;
                }

                return CompletableFuture.completedFuture(readChange);
            } else {
                // Maybe inject an error.
                CompletableFuture<Optional<RawChange>> injectedError = cqlErrorStrategy.injectFailure(Optional.empty());
                if (injectedError != null) {
                    failureCount.incrementAndGet();
                    return injectedError;
                }

                finishedReaders.add(task);
                return CompletableFuture.completedFuture(Optional.empty());
            }
        }
    }

    @Override
    public void prepare(Set<TableName> tables) {
        // No-op
    }

    @Override
    public CompletableFuture<Reader> createReader(Task task) {
        createReaderInvocations.add(task);

        long taskStartMs = task.state.getWindowStartTimestamp().toDate().getTime();
        long taskEndMs = task.state.getWindowEndTimestamp().toDate().getTime();
        Optional<ChangeId> lastConsumedChangeId = task.state.getLastConsumedChangeId();

        List<RawChange> collectedChanges = rawChanges.stream().filter(change -> {
            // FIXME: Also check table name.

            if (task.streams.stream().noneMatch(s -> s.equals(change.getId().getStreamId()))) {
                return false;
            }

            long changeTimestampMs = change.getId().getChangeTime().getDate().getTime();
            if (changeTimestampMs < taskStartMs || changeTimestampMs >= taskEndMs) {
                return false;
            }

            if (lastConsumedChangeId.isPresent()) {
                return change.getId().compareTo(lastConsumedChangeId.get()) > 0;
            }

            return true;
        }).collect(Collectors.toList());

        return CompletableFuture.completedFuture(new MockReaderCQL(task, collectedChanges.iterator()));
    }

    @Override
    public CompletableFuture<Optional<Long>> fetchTableTTL(TableName tableName) {
        Optional<Long> ttl = tablesTTL.getOrDefault(tableName, Optional.empty());
        return CompletableFuture.completedFuture(ttl);
    }

    public void setRawChanges(List<MockRawChange> rawChanges) {
        this.rawChanges = rawChanges.stream().sorted(
                Comparator.comparing(RawChange::getId)
                        .thenComparingInt(RawChange::getBatchSequenceNumber)).collect(Collectors.toList());
    }

    public void setTablesTTL(Map<TableName, Optional<Long>> tablesTTL) {
        this.tablesTTL = tablesTTL;
    }

    public boolean wasCreateReaderInvoked(Task task) {
        return createReaderInvocations.contains(task);
    }

    public Collection<Task> getCreateReaderInvocations(ChangeId changeId) {
        return createReaderInvocations.stream().filter(t -> t.streams.contains(changeId.getStreamId())).collect(Collectors.toSet());
    }

    public boolean isReaderFinished(Task task) {
        return finishedReaders.contains(task);
    }

    public void setCQLErrorStrategy(ErrorInject errorStrategy) {
        this.cqlErrorStrategy = Preconditions.checkNotNull(errorStrategy);
    }

    public int getFailureCount() {
        return failureCount.get();
    }
}
