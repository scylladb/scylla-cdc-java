package com.scylladb.cdc.model.master;

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
        this.masterConfiguration = masterConfiguration;
        for (TableName table : masterConfiguration.tables) {
            tableControllers.put(table, new TableCDCController(table, masterConfiguration));
        }
    }

    @Override
    public void runMasterStep() throws InterruptedException, ExecutionException {
        for (TableCDCController controller : tableControllers.values()) {
            TableName table = controller.getTable();
            logger.atFine().log("Processing table: %s", table);
            controller.initCurrentGeneration();
        }

        // Wait for any table's generation to be done
        while (!Thread.interrupted()) {
            Thread.sleep(masterConfiguration.sleepBeforeGenerationDoneMs);

            // Check if any table's generation is done or needs refresh
            for (TableCDCController controller : tableControllers.values()) {
                controller.runMasterStep();
            }
        }
    }
}
