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

    // Map: TableName -> TableCDCController
    private final Map<TableName, TableCDCController> tableControllers = new HashMap<>();
    private final MasterConfiguration masterConfiguration;

    public TabletBasedCDCMetadataModel(Set<TableName> tables, MasterConfiguration masterConfiguration) {
        this.masterConfiguration = masterConfiguration;
        for (TableName table : tables) {
            tableControllers.put(table, new TableCDCController(table, masterConfiguration));
        }
    }

    public static CDCMetadataModel getCurrentCDCMetadataModel(MasterConfiguration masterConfiguration) throws InterruptedException, ExecutionException {
        TabletBasedCDCMetadataModel m = new TabletBasedCDCMetadataModel(masterConfiguration.tables, masterConfiguration);
        m.initCurrentGenerations();
        return m;
    }

    public void initCurrentGenerations() throws InterruptedException, ExecutionException {
        for (TableCDCController controller : tableControllers.values()) {
            controller.initCurrentGeneration();
        }
    }

    @Override
    public void runMasterStep() throws InterruptedException, ExecutionException {
        while (!Thread.currentThread().isInterrupted()) {
            // Process all tables one by one
            for (TableCDCController controller : tableControllers.values()) {
                TableName table = controller.getTable();
                logger.atFine().log("Processing table: %s", table);

                controller.skipToNextGeneration();
                controller.configureWorkers();
            }

            // Wait for any table's generation to be done
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(masterConfiguration.sleepBeforeGenerationDoneMs);

                // Check if any table's generation is done or needs refresh
                for (TableCDCController controller : tableControllers.values()) {

                    controller.refreshEnd();

                    if (controller.skipToNextGeneration()) {
                        controller.configureWorkers();
                    }
                }
            }
        }
    }
}