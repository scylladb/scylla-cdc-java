package com.scylladb.cdc.cql.error.worker;

import com.scylladb.cdc.model.worker.RawChange;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * An <code>ErrorInject</code> implementation
 * that never injects an error.
 */
public class NoOpErrorInject implements ErrorInject {
    @Override
    public CompletableFuture<Optional<RawChange>> injectFailure(Optional<RawChange> readChange) {
        return null;
    }
}
