package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.ResultSet;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.Task;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

// Created for alterTableBeforeNextPage test method
public class MockDriver3WorkerCQL extends Driver3WorkerCQL {
    private final Semaphore proceedGate = new Semaphore(0);

    public MockDriver3WorkerCQL(Driver3Session driver3Session) {
        super(driver3Session);
    }

    public Semaphore getProceedGate() {
        return proceedGate;
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
                            // Extract the ResultSet and lastChangeId from the original reader
                            java.lang.reflect.Field rsField = Driver3WorkerCQL.Driver3Reader.class.getDeclaredField("rs");
                            rsField.setAccessible(true);
                            ResultSet rs = (ResultSet) rsField.get(originalReader);

                            java.lang.reflect.Field lastChangeIdField = Driver3WorkerCQL.Driver3Reader.class.getDeclaredField("lastChangeId");
                            lastChangeIdField.setAccessible(true);
                            @SuppressWarnings("unchecked")
                            Optional<ChangeId> lastChangeId = (Optional<ChangeId>) lastChangeIdField.get(originalReader);

                            // Create a MockDriver3Reader with the same state
                            return new MockDriver3Reader(rs, lastChangeId, proceedGate);
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
        private final ResultSet rs;
        private final Semaphore proceedGate;

        public MockDriver3Reader(ResultSet rs, Optional<ChangeId> lastChangeId, Semaphore proceedGate) {
            super(rs, lastChangeId);
            this.rs = rs;
            this.proceedGate = proceedGate;
        }

        @Override
        public CompletableFuture<Optional<RawChange>> nextChange() {
            CompletableFuture<Optional<RawChange>> result = new CompletableFuture<>();
            CompletableFuture<Void> waitForGate = null;
            if (rs.getAvailableWithoutFetching() == 0 && !rs.isFullyFetched()) {
                // Wait for the signal before proceeding (asynchronously)
                waitForGate = CompletableFuture.runAsync(() -> {
                    try {
                        proceedGate.acquire();
                        proceedGate.release();
                    } catch (InterruptedException e) {
                        System.out.println("Mock caught interrupted exception");
                        Thread.currentThread().interrupt();
                        result.completeExceptionally(e);
                    }
                });
            }
            if (waitForGate == null) {
                waitForGate = CompletableFuture.completedFuture(null);
            }
            waitForGate.whenComplete((na, ex) -> {
                if (ex != null) {
                    result.completeExceptionally(ex);
                } else {
                    try {
                        java.lang.reflect.Method findNextMethod = Driver3WorkerCQL.Driver3Reader.class.getDeclaredMethod(
                            "findNext", CompletableFuture.class);
                        findNextMethod.setAccessible(true);
                        findNextMethod.invoke(this, result);
                    } catch (Exception e) {
                        System.out.println("Failed to invoke chained findNext");
                        result.completeExceptionally(new RuntimeException("Failed to invoke findNext", e));
                    }
                }
            });
            return result;
        }
    }
}
