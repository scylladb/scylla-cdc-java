package com.scylladb.cdc.cql.error.worker;

import com.scylladb.cdc.model.worker.RawChange;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An <code>ErrorInject</code> strategy that allows
 * for error injection for a specific <code>RawChange</code>.
 * <p>
 * The {@link #requestRawChangeError(RawChange)} method allows
 * for specifying a moment this class will inject an error.
 * <p>
 * This class is thread-safe.
 *
 * @see ContinuousChangeErrorInject
 */
public class OnceChangeErrorInject implements ErrorInject {
    private final Set<RawChange> requestedRawChangeError = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * Requests an injection of error at a specific
     * change. The error injection will only
     * happen once, meaning if the same change
     * is seen the second time, no failure will
     * happen.
     *
     * @param rawChange the change that causes an injection of an error.
     * @see ContinuousChangeErrorInject#requestRawChangeError(RawChange) 
     */
    public void requestRawChangeError(RawChange rawChange) {
        requestedRawChangeError.add(rawChange);
    }

    @Override
    public CompletableFuture<Optional<RawChange>> injectFailure(Optional<RawChange> readChange) {
        if (readChange.isPresent() && requestedRawChangeError.remove(readChange.get())) {
            CompletableFuture<Optional<RawChange>> injectedFailure = new CompletableFuture<>();
            injectedFailure.completeExceptionally(new RuntimeException("Injected specific change error in Worker."));
            return injectedFailure;
        }
        return null;
    }
}
