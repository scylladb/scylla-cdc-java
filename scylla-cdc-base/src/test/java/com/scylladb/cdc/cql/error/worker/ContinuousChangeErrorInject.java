package com.scylladb.cdc.cql.error.worker;

import com.scylladb.cdc.model.worker.RawChange;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An <code>ErrorInject</code> strategy that allows
 * for error injection for a specific <code>RawChange</code>
 * until it is manually stopped.
 * <p>
 * The {@link #requestRawChangeError(RawChange)} method allows
 * for specifying a moment this class will inject an error. This
 * error will happen continuously each time the same change
 * is seen. The {@link #cancelRequestRawChangeError(RawChange)}
 * allows for canceling the error injection request.
 * <p>
 * This class is thread-safe.
 *
 * @see OnceChangeErrorInject
 */
public class ContinuousChangeErrorInject implements ErrorInject {
    private final Set<RawChange> requestedRawChangeError = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * Requests an injection of error at a specific
     * change. The error injection will
     * happen continuously - every time the same
     * change is seen again.
     *
     * @param rawChange the change that causes an injection of an error.
     * @see OnceChangeErrorInject#requestRawChangeError(RawChange)
     */
    public void requestRawChangeError(RawChange rawChange) {
        requestedRawChangeError.add(rawChange);
    }

    /**
     * Cancels a request for injection of error
     * at a specific change. After this method
     * is called, no error injection will happen
     * for the given change.
     *
     * @param rawChange the change to cancel a error injection request for.
     */
    public void cancelRequestRawChangeError(RawChange rawChange) {
        requestedRawChangeError.remove(rawChange);
    }

    @Override
    public CompletableFuture<Optional<RawChange>> injectFailure(Optional<RawChange> readChange) {
        if (readChange.isPresent() && requestedRawChangeError.contains(readChange.get())) {
            CompletableFuture<Optional<RawChange>> injectedFailure = new CompletableFuture<>();
            injectedFailure.completeExceptionally(new RuntimeException("Injected constant specific change error in Worker."));
            return injectedFailure;
        }
        return null;
    }

}
