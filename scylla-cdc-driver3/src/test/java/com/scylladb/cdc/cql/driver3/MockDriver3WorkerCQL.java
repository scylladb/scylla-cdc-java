package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.ResultSet;
import com.scylladb.cdc.cql.WorkerCQL;
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
    public final ReentrantReadWriteLock nextPageLock = new ReentrantReadWriteLock();
    public final ReentrantReadWriteLock nextRowLock = new ReentrantReadWriteLock();
    public MockDriver3WorkerCQL(Driver3Session driver3Session) {
        super(driver3Session);
    }


    @Override
    public CompletableFuture<WorkerCQL.Reader> createReader(Task task) {
        CompletableFuture<WorkerCQL.Reader> result = super.createReader(task);

        // When the result is available, replace Driver3Reader instances with MockDriver3Reader
        return result.thenApply(reader -> {
            try {
                // Access the Driver3MultiReader's readers field using reflection
                java.lang.reflect.Field readersField = reader.getClass().getDeclaredField("readers");
                readersField.setAccessible(true);

                @SuppressWarnings("unchecked")
                List<Driver3WorkerCQL.Driver3Reader> originalReaders =
                    (List<Driver3WorkerCQL.Driver3Reader>) readersField.get(reader);

                // Replace each Driver3Reader with MockDriver3Reader
                List<MockDriver3Reader> mockReaders = originalReaders.stream()
                    .map(originalReader -> {
                        try {
                            // Create a MockDriver3Reader with the same state
                            return new MockDriver3Reader(originalReader.rs, originalReader.lastChangeId, nextPageLock, nextRowLock);
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to create MockDriver3Reader", e);
                        }
                    })
                    .collect(Collectors.toList());

                // Set the mock readers back into the Driver3MultiReader
                readersField.set(reader, mockReaders);

                return reader;
            } catch (Exception e) {
                throw new RuntimeException("Failed to replace Driver3Reader instances", e);
            }
        });
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
            CompletableFuture.runAsync( () -> {
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
            });
        }
    }
}
