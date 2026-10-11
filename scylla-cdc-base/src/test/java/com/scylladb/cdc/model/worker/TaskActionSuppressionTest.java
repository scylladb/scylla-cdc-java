package com.scylladb.cdc.model.worker;

import com.scylladb.cdc.cql.MockWorkerCQL;
import com.scylladb.cdc.transport.MockWorkerTransport;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskActionSuppressionTest {
    @Test
    void classifiesNoisyFailureAfterCompletableFutureStagesWrapIt() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            RuntimeException cause = new RuntimeException("transient CQL failure");
            WorkerConfiguration config = WorkerConfiguration.builder()
                    .withCQL(new MockWorkerCQL() {
                        @Override
                        public boolean isNoisyException(Throwable exception) {
                            return exception == cause;
                        }
                    })
                    .withTransport(new MockWorkerTransport())
                    .withConsumer(Consumer.syncRawChangeConsumer(change -> {}))
                    .withExecutorService(executor)
                    .withNoisyExceptionSuppressionWindowMs(60_000)
                    .build();
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(cause);
            Throwable wrapped = failed.thenApply(value -> value).handle((value, error) -> error).join();
            assertTrue(wrapped instanceof CompletionException);

            assertFalse(TaskAction.shouldSuppressLog(wrapped, config, "table_a"));
            assertTrue(TaskAction.shouldSuppressLog(wrapped, config, "table_a"));
            assertFalse(TaskAction.shouldSuppressLog(wrapped, config, "table_b"));
            assertFalse(TaskAction.shouldSuppressLog(new RuntimeException("other"), config, "table_a"));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void configurationRebuildRetainsSuppressionWindow() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            WorkerConfiguration.Builder builder = WorkerConfiguration.builder()
                    .withCQL(new MockWorkerCQL())
                    .withTransport(new MockWorkerTransport())
                    .withConsumer(Consumer.syncRawChangeConsumer(change -> {}))
                    .withExecutorService(executor)
                    .withClock(Clock.fixed(Instant.ofEpochMilli(1000), ZoneOffset.UTC))
                    .withNoisyExceptionSuppressionWindowMs(60_000);
            WorkerConfiguration first = builder.build();
            assertFalse(first.noisyExceptionSuppressor.shouldSuppress("table_a"));

            builder.withClock(Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));
            WorkerConfiguration replacement = builder.build();
            assertSame(first.noisyExceptionSuppressor, replacement.noisyExceptionSuppressor);
            assertTrue(replacement.noisyExceptionSuppressor.shouldSuppress("table_a"));
        } finally {
            executor.shutdownNow();
        }
    }
}
