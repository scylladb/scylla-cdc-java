package com.scylladb.cdc.model.master;

import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.model.TableName;
import java.util.*;
import java.util.concurrent.*;

/**
 * Tablet-based CDC metadata model.
 * All tables are managed in a single thread.
 */
public class TabletBasedCDCMetadataModel implements CDCMetadataModel {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final Map<TableName, TableCDCController> tableControllers = new HashMap<>();
    private final MasterConfiguration masterConfiguration;

    public TabletBasedCDCMetadataModel(MasterConfiguration masterConfiguration) {
        this.masterConfiguration = Preconditions.checkNotNull(masterConfiguration);

        for (TableName table : masterConfiguration.tables) {
            tableControllers.put(table, new TableCDCController(table, masterConfiguration));
        }
    }

    @Override
    public void runMasterLoop() throws InterruptedException, ExecutionException {

        // Stop existing workers if any are left from a previous run.
        masterConfiguration.transport.stopWorkers();

        for (TableCDCController controller : tableControllers.values()) {
            controller.initCurrentGeneration();
        }

        while (!Thread.currentThread().isInterrupted()) {
            Thread.sleep(masterConfiguration.sleepBeforeGenerationDoneMs);

            // Check if any table's generation is done or needs refresh
            for (TableCDCController controller : tableControllers.values()) {
                controller.runMasterStep();
            }
        }
    }
}
