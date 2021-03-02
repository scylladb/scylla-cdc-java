package com.scylladb.cdc.model.worker;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class DelayedFutureService {
    private static final int AWAIT_TIMEOUT_MS = 1000;

    private final ScheduledExecutorService internalExecutor;
    private final Set<CompletableFuture<Void>> scheduledFutures;

    public DelayedFutureService() {
        this.scheduledFutures = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.internalExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread t = new Thread(runnable);
            t.setName("DelayedFutureServiceThreadFor" + Thread.currentThread().getName());
            t.setDaemon(true);
            return t;
        });
    }

    public CompletableFuture<Void> delayedFuture(long numberOfMilliseconds) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        scheduledFutures.add(result);
        internalExecutor.schedule(() -> {
            result.complete(null);
            scheduledFutures.remove(result);
        }, numberOfMilliseconds, TimeUnit.MILLISECONDS);
        return result;
    }

    public void stop() throws InterruptedException {
        internalExecutor.shutdown();
        internalExecutor.awaitTermination(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        for (CompletableFuture<Void> scheduledFuture : scheduledFutures) {
            scheduledFuture.complete(null);
        }
    }
}
