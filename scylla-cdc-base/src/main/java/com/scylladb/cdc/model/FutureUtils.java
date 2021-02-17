package com.scylladb.cdc.model;

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
}
