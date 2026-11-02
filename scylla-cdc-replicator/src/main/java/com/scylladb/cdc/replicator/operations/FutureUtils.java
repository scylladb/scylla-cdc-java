package com.scylladb.cdc.replicator.operations;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.google.common.flogger.FluentLogger;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

public final class FutureUtils {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private FutureUtils() {
        throw new UnsupportedOperationException(FutureUtils.class.getName() + " should never be instantiated");
    }

    public static <T> CompletableFuture<Void> convert(ListenableFuture<T> fut, String errorMsg) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        Futures.addCallback(fut, new FutureCallback<T>() {
            @Override
            public void onSuccess(T ignored) {
                result.complete(null);
            }

            @Override
            public void onFailure(Throwable t) {
                logger.atSevere().withCause(t).log(errorMsg);
                // Fail the change so the task re-reads it instead of moving past it.
                result.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());

        return result;
    }

    public static <T, R> CompletableFuture<R> transformDeferred(ListenableFuture<T> fut,
                                                                Function<T, CompletableFuture<R>> consumer) {
        CompletableFuture<R> result = new CompletableFuture<>();

        Futures.addCallback(fut, new FutureCallback<T>() {
            @Override
            public void onSuccess(T input) {
                try {
                    consumer.apply(input).whenComplete((r, e) -> {
                        if (e != null) {
                            result.completeExceptionally(e);
                        } else {
                            result.complete(r);
                        }
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                    result.completeExceptionally(e);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                t.printStackTrace();
                result.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor()); // Runs synchronously, avoids requiring an executor parameter

        return result;
    }

}