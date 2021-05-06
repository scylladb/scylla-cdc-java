package com.scylladb.cdc.printer;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import com.google.common.io.BaseEncoding;
import com.scylladb.cdc.lib.CDCConsumer;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.BatchSequence;
import com.scylladb.cdc.model.worker.BatchSequenceConsumer;
import com.scylladb.cdc.model.worker.Change;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.ChangeTime;
import com.scylladb.cdc.model.worker.Consumer;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.RawChange.OperationType;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import com.scylladb.cdc.model.worker.cql.Cell;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class Main {
    public static void main(String[] args) {
        // Get connection info and desired table
        // from command line arguments.
        Namespace parsedArguments = parseArguments(args);
        String source = parsedArguments.getString("source");
        String keyspace = parsedArguments.getString("keyspace"), table = parsedArguments.getString("table");
        boolean events = parsedArguments.getBoolean("events");

        // Build a consumer. The CDCConsumer instance
        // can be run in multi-thread setting and the consumer is shared
        // across.
        //
        // You should provide
        // a class that implements the RawChangeConsumer
        // or any of the other consumer interfaces.
        //
        // Here, we lambdas for simplicity.
        //
        // The consume() method of RawChangeConsumer returns
        // a CompletableFuture, so your code can perform
        // some I/O responding to the change.
        //
        // The RawChange name alludes to the fact that
        // changes represented by this class correspond
        // 1:1 to rows in *_scylla_cdc_log table.
        //
        // For automatic grouping of change rows
        // into associated batch sequence, we can use
        // the BatchSequenceConsumer instead.

        Consumer c;

        if (!events) {
            RawChangeConsumer rc = change -> {
                // Print the change. See printChange()
                // for more information on how to
                // access its details.
                printChange(change);
                return CompletableFuture.completedFuture(null);
            };
            c = Consumer.forRawChangeConsumer(rc);
        } else {
            BatchSequenceConsumer bc = e -> {
                printSequence(e);
                return CompletableFuture.completedFuture(null);
            };
            c = Consumer.forBatchSequenceConsumer(bc);
        }

        // Build a CDCConsumer, which is single-threaded
        // (workersCount(1)), reads changes
        // from [keyspace].[table] and passes them
        // to consumers created by changeConsumerProvider.
        try (CDCConsumer consumer = CDCConsumer.builder()
                .addContactPoint(source)
                .addTable(new TableName(keyspace, table))
                .withConsumer(c)
                .withWorkersCount(1)
                .build()) {

            // Start a consumer. You can stop it by using .stop() method
            // or it can be automatically stopped when created in a
            // try-with-resources (as shown above).
            consumer.start();

            // The consumer is started in background threads.
            // It is consuming the CDC log and providing read changes
            // to the consumers.

            waitForCtrlC(consumer); // helper
        } catch (InterruptedException ex) {
            System.err.println("Exception occurred while running the Printer: "
                + ex.getMessage());
        }

        // The CDCConsumer is gracefully stopped after try-with-resources.
    }

    private static void waitForCtrlC(CDCConsumer consumer) throws InterruptedException {
        CountDownLatch terminationLatch = new CountDownLatch(1);
        CountDownLatch terminatedLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            terminationLatch.countDown();
            try {
                terminatedLatch.await();
            } catch (InterruptedException e1) {
            }
        }));
        terminationLatch.await();
        consumer.stop();
        terminatedLatch.countDown();
    }

    private static enum Type {
        Row, Event
    }

    private static void printSequence(BatchSequence sequence) {
        printChange(Type.Event, sequence, null, sequence.getChanges());
    }

    private static void printChange(RawChange change) {
        printChange(Type.Row, change, change.getOperationType(), Collections.singleton(change));
    }

    private static void printChange(Type type, Change change, OperationType optype, Collection<RawChange> rows) {
        // Get the ID of the change which contains stream_id and time.
        ChangeId changeId = change.getId();
        StreamId streamId = changeId.getStreamId();
        ChangeTime changeTime = changeId.getChangeTime();

        prettyPrintChangeHeader(type, streamId, changeTime);

        OperationType last = optype;
        if (last != null) {
            prettyPrintChange(last);
        } else {
            printSeparator();
        }

        // In each RawChange there is an information about the
        // change schema.
        ChangeSchema changeSchema = change.getSchema();

        // There are two types of columns inside the ChangeSchema:
        // - CDC log columns (cdc$time, cdc$stream_id, ...)
        // - base table columns
        //
        // CDC log columns can be easily accessed by RawChange
        // helper methods (such as getTTL(), getId()).
        //
        // Let's concentrate on non-CDC columns (those are
        // from the base table) and iterate over them:
        List<ChangeSchema.ColumnDefinition> nonCdcColumnDefinitions = changeSchema.getNonCdcColumnDefinitions();
        int row = 0;
        for (RawChange c : rows) {
            if (type == Type.Event) {
                if (row++ > 0) {
                    printSeparator();
                }
                OperationType t = c.getOperationType();
                if (t != last) {
                    prettyPrintChange(t);
                }
                last = t;
            }

            int columnIndex = 0; // For pretty printing.

            for (ChangeSchema.ColumnDefinition columnDefinition : nonCdcColumnDefinitions) {
                String columnName = columnDefinition.getColumnName();

                // We can get information if this column was a part of primary
                // key
                // in the base table. Note that in CDC log table different
                // columns
                // are part of primary key (cdc$stream_id, cdc$time,
                // batch_seq_no).
                ChangeSchema.ColumnType baseTableColumnType = columnDefinition.getBaseTableColumnType();

                // Get the information about the data type (as present in CDC
                // log).
                ChangeSchema.DataType logDataType = columnDefinition.getCdcLogDataType();

                // Finally, we can get the value of this column:
                Cell cell = c.getCell(columnName);

                // Depending on the logDataType, you will want
                // to use different methods of Cell, for example
                // cell.getInt() if column is of INT type:
                //
                // Integer value = cell.getInt();
                //
                // getInt() can return null, if the cell value
                // was NULL.
                //
                // For printing purposes, we use getAsObject():
                Object cellValue = cell.getAsObject();

                prettyPrintCell(columnName, baseTableColumnType, logDataType, cellValue,
                        (++columnIndex == nonCdcColumnDefinitions.size()));
            }
        }

        prettyPrintEnd();
    }

    // Some pretty printing helpers:

    private static void prettyPrintChangeHeader(Type type, StreamId streamId, ChangeTime changeTime) {
        byte[] buf = new byte[16];
        streamId.getValue().duplicate().get(buf, 0, 16);

        switch (type) {
        case Row:
            System.out.println("┌────────────────── Scylla CDC log row ──────────────────┐");
            break;
        case Event:
            System.out.println("┌───────────────── Scylla CDC log event ─────────────────┐");
            break;
        }

        prettyPrintField("Stream id:", BaseEncoding.base16().encode(buf, 0, 16));
        prettyPrintField("Timestamp:", new SimpleDateFormat("dd/MM/yyyy, HH:mm:ss.SSS").format(changeTime.getDate()));
    }

    private static void prettyPrintChange(RawChange.OperationType operationType) {
        prettyPrintField("Operation type:", operationType.name());
        printSeparator();
    }

    private static void printSeparator() {
        System.out.println("├────────────────────────────────────────────────────────┤");
    }

    private static void prettyPrintCell(String columnName, ChangeSchema.ColumnType baseTableColumnType,
            ChangeSchema.DataType logDataType, Object cellValue, boolean isLast) {
        prettyPrintField(columnName + ":", Objects.toString(cellValue));
        prettyPrintField(columnName + " (schema):",
                columnName + ", " + logDataType.toString() + ", " + baseTableColumnType.name());
        if (!isLast) {
            prettyPrintField("", "");
        }
    }

    private static void prettyPrintEnd() {
        System.out.println("└────────────────────────────────────────────────────────┘");
        System.out.println();
    }

    private static void prettyPrintField(String fieldName, String fieldValue) {
        System.out.print("│ " + fieldName + " ");

        int leftSpace = 53 - fieldName.length();

        if (fieldValue.length() > leftSpace) {
            fieldValue = fieldValue.substring(0, leftSpace - 3) + "...";
        }

        fieldValue = String.format("%1$" + leftSpace + "s", fieldValue);
        System.out.println(fieldValue + " │");
    }

    // Parsing the command-line arguments:

    private static Namespace parseArguments(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("./scylla-cdc-printer").build().defaultHelp(true);
        parser.addArgument("-k", "--keyspace").required(true).help("Keyspace name");
        parser.addArgument("-t", "--table").required(true).help("Table name");
        parser.addArgument("-s", "--source").required(true).setDefault("127.0.0.1")
                .help("Address of a node in source cluster");
        parser.addArgument("-e", "--events").required(false).action(Arguments.storeConst()).setConst(true)
                .setDefault(false).help("Print CDC batch sequence events");

        try {
            return parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(-1);
            return null;
        }
    }
}
