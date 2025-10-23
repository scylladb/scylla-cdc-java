package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.Task;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

// Created for alterTableBeforeNextPage test method
public class MockDriver3WorkerCQL extends Driver3WorkerCQL {
    public final ReentrantReadWriteLock nextPageLock;
    public final ReentrantReadWriteLock nextRowLock;

    public MockDriver3WorkerCQL(Driver3Session driver3Session) {
        this(driver3Session, new ReentrantReadWriteLock(), new ReentrantReadWriteLock());
    }

    public MockDriver3WorkerCQL(Driver3Session driver3Session, ReentrantReadWriteLock nextPageLock, ReentrantReadWriteLock nextRowLock) {
        super(driver3Session);
        this.nextPageLock = nextPageLock;
        this.nextRowLock = nextRowLock;
    }

    public class MockDriver3Reader extends Driver3WorkerCQL.Driver3Reader {
        // Write lock will be used for blocking from outside.
        // Read lock will be used inside the worker.
        public final ReadWriteLock nextPageLock;
        public final ReadWriteLock nextRowLock;

        public MockDriver3Reader(ResultSet rs, Optional<ChangeId> lastChangeId, ReadWriteLock nextPageLock, ReadWriteLock nextRowLock) {
            super(rs, lastChangeId);
            this.nextPageLock = nextPageLock;
            this.nextRowLock = nextRowLock;
        }

        @Override
        protected void findNext(CompletableFuture<Optional<RawChange>> fut) {
            if (rs.getAvailableWithoutFetching() == 0 && !rs.isFullyFetched()) {
                nextPageLock.readLock().lock();
                try {
                    super.findNext(fut);
                } finally {
                    nextPageLock.readLock().unlock();
                }
            } else {
                nextRowLock.readLock().lock();
                try {
                    super.findNext(fut);
                } finally {
                    nextRowLock.readLock().unlock();
                }
            }
        }
    }

    public class MockDriver3MultiReader extends Driver3WorkerCQL.Driver3MultiReader {
        public MockDriver3MultiReader(List<ResultSet> rss, Optional<ChangeId> lastChangeId) {
          super(rss, lastChangeId);
          this.readers = rss.stream().map(rs -> new MockDriver3Reader(rs, lastChangeId, nextPageLock, nextRowLock)).collect(Collectors.toList());
        }
    }

    @Override
    protected CompletableFuture<Reader> wrapResults(Task task, List<ResultSetFuture> futures) {
        CompletableFuture<Reader> result = new CompletableFuture<>();
        Futures.addCallback(Futures.allAsList(futures), new FutureCallback<List<ResultSet>>() {
            @Override
            public void onSuccess(List<ResultSet> rss) {
                result.complete(new MockDriver3MultiReader(rss, task.state.getLastConsumedChangeId()));
            }

            @Override
            public void onFailure(Throwable t) {
                result.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
        return result;
    }
}
