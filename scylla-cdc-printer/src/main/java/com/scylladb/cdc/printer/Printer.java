package com.scylladb.cdc.printer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.scylladb.cdc.lib.CDCConsumer;
import com.scylladb.cdc.lib.CDCConsumerBuilder;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import sun.misc.Signal;

import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Printer {
    public static void main(String[] args) {
        turnOffLogging();

        try (Cluster c = Cluster.builder().addContactPoint("127.0.0.2").build(); Session s = c.connect()) {
            HashSet<TableName> tables = new HashSet<>();
            tables.add(new TableName("ks", "t"));

            CDCConsumer consumer = CDCConsumerBuilder.builder(s, change -> {
                printChange(change);
                return CompletableFuture.completedFuture(null);
            }, tables).workersCount(1).build();

            consumer.start();
            waitForConsumerStop(consumer);
        }
    }

    private static void printChange(RawChange change) {
        System.out.println(change.getId());

        ChangeSchema changeSchema = change.getSchema();
        System.out.println("Cdc columns:");
        for (ChangeSchema.ColumnDefinition cd : changeSchema.getCdcColumnDefinitions()) {
            System.out.println(cd.getColumnName() + " " + cd.getCdcLogDataType());
        }

        System.out.println();
        System.out.println("Non-cdc columns:");
        for (ChangeSchema.ColumnDefinition cd : changeSchema.getNonCdcColumnDefinitions()) {
            System.out.println(cd.getColumnName() + " " + cd.getCdcLogDataType() + " " + cd.getBaseTableColumnType());
            System.out.print("With value: ");
            System.out.println(change.getAsObject(cd.getColumnName()));
        }
    }

    private static void turnOffLogging() {
        Logger root = Logger.getLogger("");
        root.setLevel(Level.OFF);
        for (Handler handler : root.getHandlers()) {
            handler.setLevel(Level.OFF);
        }
    }

    private static void waitForConsumerStop(CDCConsumer consumer) {
        try {
            CountDownLatch terminationLatch = new CountDownLatch(1);
            Signal.handle(new Signal("INT"), signal -> terminationLatch.countDown());
            terminationLatch.await();
            consumer.stop();
        } catch (InterruptedException e) {
            // Ignore exception.
        }
    }
}
