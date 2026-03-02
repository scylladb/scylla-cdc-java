package com.scylladb.cdc.cql;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.master.GenerationMetadata;

public interface MasterCQL {

    CompletableFuture<Optional<Long>> fetchTableTTL(TableName tableName);
    CompletableFuture<Optional<Throwable>> validateTable(TableName table);
    Boolean usesTablets(TableName tableName);

    // Vnode-based CDC methods
    CompletableFuture<Optional<GenerationId>> fetchFirstGenerationId();

    /**
     * Fetches the first vnode-based CDC generation whose start timestamp is
     * strictly after the given date.
     *
     * @param after exclusive lower bound for the generation start timestamp
     * @return the earliest generation after the given date, or empty if none exists
     */
    default CompletableFuture<Optional<GenerationId>> fetchFirstGenerationIdAfter(Date after) {
        throw new UnsupportedOperationException("fetchFirstGenerationIdAfter is not implemented; "
                + "required for catch-up optimization (withCatchUpWindowSizeSeconds > 0)");
    }
    CompletableFuture<GenerationMetadata> fetchGenerationMetadata(GenerationId id);
    CompletableFuture<Optional<Timestamp>> fetchGenerationEnd(GenerationId id);

    /**
     * Fetches the latest vnode-based CDC generation whose start timestamp is
     * at or before the given cutoff date. Used by the catch-up optimization
     * to jump directly to a recent generation on first startup.
     *
     * @param cutoff inclusive upper bound for the generation start timestamp
     * @return the latest generation at or before the cutoff, or empty if none exists
     */
    default CompletableFuture<Optional<GenerationId>> fetchLastGenerationBeforeOrAt(Date cutoff) {
        throw new UnsupportedOperationException("fetchLastGenerationBeforeOrAt is not implemented; "
                + "required for catch-up optimization (withCatchUpWindowSizeSeconds > 0)");
    }

    /**
     * Fetches the latest tablet-based CDC generation for the given table whose
     * start timestamp is at or before the given cutoff date. Used by the catch-up
     * optimization to jump directly to a recent generation on first startup.
     *
     * @param table the table to look up generations for
     * @param cutoff inclusive upper bound for the generation start timestamp
     * @return the latest generation at or before the cutoff, or empty if none exists
     */
    default CompletableFuture<Optional<GenerationId>> fetchLastTableGenerationBeforeOrAt(TableName table, Date cutoff) {
        throw new UnsupportedOperationException("fetchLastTableGenerationBeforeOrAt is not implemented; "
                + "required for catch-up optimization (withCatchUpWindowSizeSeconds > 0)");
    }

    // Tablet-based CDC methods
    //
    // Note: these return non-Optional GenerationId (unlike the vnode methods above)
    // because for tablet-based CDC, a generation must always exist for a CDC-enabled
    // table. A missing generation indicates an error condition and is surfaced as an
    // IllegalStateException via the future.

    CompletableFuture<GenerationId> fetchFirstTableGenerationId(TableName table);

    /**
     * Fetches the first tablet-based CDC generation for the given table whose
     * start timestamp is strictly after the given date.
     *
     * @param table the table to look up generations for
     * @param after exclusive lower bound for the generation start timestamp
     * @return the earliest generation after the given date
     * @throws IllegalStateException (via the future) if no generation exists after the given date
     */
    default CompletableFuture<GenerationId> fetchFirstTableGenerationIdAfter(TableName table, Date after) {
        throw new UnsupportedOperationException("fetchFirstTableGenerationIdAfter is not implemented; "
                + "required for catch-up optimization (withCatchUpWindowSizeSeconds > 0)");
    }
    CompletableFuture<GenerationMetadata> fetchTableGenerationMetadata(TableName table, GenerationId generationId);
    CompletableFuture<Optional<Timestamp>> fetchTableGenerationEnd(TableName table, GenerationId generationId);
}
