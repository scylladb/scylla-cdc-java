package com.scylladb.cdc.model.master;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.transport.MasterTransport;

public final class Master {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final MasterConfiguration masterConfiguration;

    public Master(MasterConfiguration masterConfiguration) {
        this.masterConfiguration = Preconditions.checkNotNull(masterConfiguration);
    }

    public void run() {
        if (masterConfiguration.catchUpConfig.isEnabled()) {
            logger.atInfo().log("Master starting with catch-up optimization enabled (window: %d seconds)",
                    masterConfiguration.catchUpConfig.getCatchUpWindowSizeSeconds());
        }

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

    private static boolean isTabletsBased(MasterConfiguration masterConfiguration) {
        boolean tabletsBased = false;
        boolean first = true;
        for (TableName table : masterConfiguration.tables) {
            boolean usesTablets = masterConfiguration.cql.usesTablets(table);
            if (first) {
                tabletsBased = usesTablets;
                first = false;
            } else {
                if (tabletsBased != usesTablets) {
                    throw new IllegalArgumentException(String.format(
                        "Mixed tablet configuration detected: table '%s' %s tablets, but other tables in the configuration %s tablets. " +
                        "All tables in the same CDC configuration must consistently use either tablet-based replication or vnodes-based.",
                        table, usesTablets ? "uses" : "does not use", tabletsBased ? "use" : "do not use"
                    ));
                }
            }
        }
        return tabletsBased;
    }

    // Returns the current CDC metadata model.
    private CDCMetadataModel getCurrentCDCMetadataModel() throws InterruptedException, ExecutionException {
        if (isTabletsBased(masterConfiguration)) {
            logger.atFine().log("Using TabletBasedCDCMetadataModel for CDC metadata model.");
            return new TabletBasedCDCMetadataModel(masterConfiguration);
        } else {
            logger.atFine().log("Using GenerationBasedCDCMetadataModel for CDC metadata model.");
            return new GenerationBasedCDCMetadataModel(masterConfiguration);
        }
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

    private void runUntilException() throws ExecutionException {
        try {
            CDCMetadataModel model = getCurrentCDCMetadataModel();
            model.runMasterLoop();
        } catch (InterruptedException e) {
            // Interruptions are expected.
            Thread.currentThread().interrupt();
        }
    }

}
