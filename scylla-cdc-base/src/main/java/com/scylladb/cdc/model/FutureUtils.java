package com.scylladb.cdc.model;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public final class FutureUtils {
    private static class ComposeExceptionallyWrapper<T> {
        T result;
        Throwable throwable;

        public ComposeExceptionallyWrapper(T result, Throwable throwable) {
            this.result = result;
            this.throwable = throwable;
        }
    }

    /*
     * If |fut| completes exceptionally, apply |exceptionally| function to
     * its exception. Similar to CompletableFuture's thenCompose, but
     * composing when the future completed exceptionally.
     */
    public static <T> CompletableFuture<T> thenComposeExceptionally(CompletableFuture<T> fut, Function<Throwable, ? extends CompletionStage<T>> exceptionally) {
        return fut.handle((res, ex) -> {
            return new ComposeExceptionallyWrapper(res, ex);
        }).thenCompose((wrapper) -> {
            if (wrapper.throwable == null) {
                return CompletableFuture.completedFuture((T) wrapper.result);
            } else {
                return exceptionally.apply(wrapper.throwable);
            }
        });
    }

    public static <T> CompletableFuture<T> convert(ListenableFuture<T> fut) {
        CompletableFuture<T> result = new CompletableFuture<>();
        Futures.addCallback(fut, new FutureCallback<T>() {

            @Override
            public void onSuccess(T value) {
                result.complete(value);
            }

            @Override
            public void onFailure(Throwable t) {
                result.completeExceptionally(t);
            }
        });
        return result;
    }

    public static <T> CompletableFuture<T> exceptionalFuture(Throwable t) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }
}
