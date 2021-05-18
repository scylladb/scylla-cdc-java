package com.scylladb.cdc.cql.error.worker;

import com.scylladb.cdc.model.worker.RawChange;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * This interface provides a pluggable way to
 * implement different error injection strategies
 * for <code>MockWorkerCQL</code> operations.
 * <p>
 * Classes implementing this interface should
 * be thread-safe, as the test code and started
 * <code>MockWorkerCQL</code> will operate
 * on different threads.
 */
public interface ErrorInject {
    /**
     * Returns an exceptional future if a failure was injected or
     * <code>null</code> if no failure was injected. This method
     * should never return a non-exceptional completed future.
     *
     * @param readChange the original change without any failures.
     * @return an exceptional future if a failure was injected or
     *         <code>null</code> if no failure was injected.
     */
    CompletableFuture<Optional<RawChange>> injectFailure(Optional<RawChange> readChange);
}
