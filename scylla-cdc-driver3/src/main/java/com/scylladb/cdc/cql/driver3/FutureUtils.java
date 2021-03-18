package com.scylladb.cdc.cql.driver3;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.CompletableFuture;

class FutureUtils {
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
}
