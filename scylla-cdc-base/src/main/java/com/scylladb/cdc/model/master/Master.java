package com.scylladb.cdc.model.master;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.model.TableName;

public final class Master {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final MasterConfiguration masterConfiguration;

    public Master(MasterConfiguration masterConfiguration) {
        this.masterConfiguration = Preconditions.checkNotNull(masterConfiguration);
    }

    public void run() {
        // Until the master thread is interrupted, continuously run fetching
        // the new generations. In case of exception (for example
        // CQL query error), infinitely retry the master routine from
        // the beginning, upon waiting with a fixed backoff time.
        while (!Thread.interrupted()) {
            try {
                runUntilException();
            } catch (Exception ex) {
                logger.atSevere().withCause(ex).log("Got an Exception inside Master. " +
                        "Will attempt to retry after a back-off time.");
            }
            // Retry backoff
            try {
                Thread.sleep(masterConfiguration.sleepAfterExceptionMs);
            } catch (InterruptedException e) {
                // Interruptions are expected.
                Thread.currentThread().interrupt();
            }
        }
    }

    private void runUntilException() throws ExecutionException {
        try {
            CDCMetadataModel model = getCurrentCDCMetadataModel();
            while (!Thread.interrupted()) {
                model.runMasterStep();
            }
        } catch (InterruptedException e) {
            // Interruptions are expected.
            Thread.currentThread().interrupt();
        }
    }

    // Returns the current CDC metadata model.
    private CDCMetadataModel getCurrentCDCMetadataModel() throws InterruptedException, ExecutionException {
        logger.atInfo().log("Using GenerationBasedCDCMetadataModel for CDC metadata model.");
        return GenerationBasedCDCMetadataModel.getCurrentCDCMetadataModel(masterConfiguration);
    }

    public Optional<Throwable> validate() {
        try {
            for (TableName table : masterConfiguration.tables) {
                Optional<Throwable> tableValidation = masterConfiguration.cql.validateTable(table).get();
                if (tableValidation.isPresent()) {
                    return tableValidation;
                }
            }
        } catch (InterruptedException | ExecutionException ex) {
            return Optional.of(ex);
        }
        return Optional.empty();
    }

}
