package com.scylladb.cdc.printer;

import com.google.common.io.BaseEncoding;
import com.scylladb.cdc.lib.CDCConsumer;
import com.scylladb.cdc.lib.RawChangeConsumerProvider;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.ChangeTime;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import com.scylladb.cdc.model.worker.cql.Cell;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import sun.misc.Signal;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

public class Main {
    public static void main(String[] args) {
        // Get connection info and desired table
        // from command line arguments.
        Namespace parsedArguments = parseArguments(args);
        String source = parsedArguments.getString("source");
        String keyspace = parsedArguments.getString("keyspace"), table = parsedArguments.getString("table");

        // Build a provider of consumers. The CDCConsumer instance
        // can be run in multi-thread setting and a separate
        // RawChangeConsumer is used by each thread.
        RawChangeConsumerProvider changeConsumerProvider = threadId -> {
            // Build a consumer of changes. You should provide
            // a class that implements the RawChangeConsumer
            // interface.
            //
            // Here, we use a lambda for simplicity.
            //
            // The consume() method of RawChangeConsumer returns
            // a CompletableFuture, so your code can perform
            // some I/O responding to the change.
            //
            // The RawChange name alludes to the fact that
            // changes represented by this class correspond
            // 1:1 to rows in *_scylla_cdc_log table.
            RawChangeConsumer changeConsumer = change -> {
                // Print the change. See printChange()
                // for more information on how to
                // access its details.
                printChange(change);
                return CompletableFuture.completedFuture(null);
            };
            return changeConsumer;
        };

        // Build a CDCConsumer, which is single-threaded
        // (workersCount(1)), reads changes
        // from [keyspace].[table] and passes them
        // to consumers created by changeConsumerProvider.
        try (CDCConsumer consumer = CDCConsumer.builder()
                .addContactPoint(source)
                .addTable(new TableName(keyspace, table))
                .withConsumerProvider(changeConsumerProvider)
                .withWorkersCount(1)
                .build()) {

            // Start a consumer. You can stop it by using .stop() method
            // or it can be automatically stopped when created in a
            // try-with-resources (as shown above).
            consumer.start();

            // The consumer is started in background threads.
            // It is consuming the CDC log and providing read changes
            // to the consumers.

            // Wait for SIGINT:
            CountDownLatch terminationLatch = new CountDownLatch(1);
            Signal.handle(new Signal("INT"), signal -> terminationLatch.countDown());
            terminationLatch.await();
        } catch (InterruptedException ex) {
            System.err.println("Exception occurred while running the Printer: "
                + ex.getMessage());
        }

        // The CDCConsumer is gracefully stopped after try-with-resources.
    }

    private static void printChange(RawChange change) {
        // Get the ID of the change which contains stream_id and time.
        ChangeId changeId = change.getId();
        StreamId streamId = changeId.getStreamId();
        ChangeTime changeTime = changeId.getChangeTime();

        // Get the operation type, for example: ROW_UPDATE, POST_IMAGE.
        RawChange.OperationType operationType = change.getOperationType();

        prettyPrintChangeHeader(streamId, changeTime, operationType);

        // In each RawChange there is an information about the
        // change schema.
        ChangeSchema changeSchema = change.getSchema();

        // There are two types of columns inside the ChangeSchema:
        //   - CDC log columns (cdc$time, cdc$stream_id, ...)
        //   - base table columns
        //
        // CDC log columns can be easily accessed by RawChange
        // helper methods (such as getTTL(), getId()).
        //
        // Let's concentrate on non-CDC columns (those are
        // from the base table) and iterate over them:
        List<ChangeSchema.ColumnDefinition> nonCdcColumnDefinitions = changeSchema.getNonCdcColumnDefinitions();
        int columnIndex = 0; // For pretty printing.

        for (ChangeSchema.ColumnDefinition columnDefinition : nonCdcColumnDefinitions) {
            String columnName = columnDefinition.getColumnName();

            // We can get information if this column was a part of primary key
            // in the base table. Note that in CDC log table different columns
            // are part of primary key (cdc$stream_id, cdc$time, batch_seq_no).
            ChangeSchema.ColumnKind baseTableColumnKind = columnDefinition.getBaseTableColumnKind();

            // Get the information about the data type (as present in CDC log).
            ChangeSchema.DataType logDataType = columnDefinition.getCdcLogDataType();

            // Finally, we can get the value of this column:
            Cell cell = change.getCell(columnName);

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

            prettyPrintCell(columnName, baseTableColumnKind, logDataType,
                    cellValue, (++columnIndex == nonCdcColumnDefinitions.size()));
        }

        prettyPrintEnd();
    }

    // Some pretty printing helpers:

    private static void prettyPrintChangeHeader(StreamId streamId, ChangeTime changeTime,
                                                RawChange.OperationType operationType) {
        byte[] buf = new byte[16];
        streamId.getValue().duplicate().get(buf, 0, 16);

        System.out.println("┌────────────────── Scylla CDC log row ──────────────────┐");
        prettyPrintField("Stream id:", BaseEncoding.base16().encode(buf, 0, 16));
        prettyPrintField("Timestamp:", new SimpleDateFormat("dd/MM/yyyy, HH:mm:ss.SSS").format(changeTime.getDate()));
        prettyPrintField("Operation type:", operationType.name());
        System.out.println("├────────────────────────────────────────────────────────┤");
    }

    private static void prettyPrintCell(String columnName, ChangeSchema.ColumnKind baseTableColumnKind, ChangeSchema.DataType logDataType, Object cellValue, boolean isLast) {
        prettyPrintField(columnName + ":", Objects.toString(cellValue));
        prettyPrintField(columnName + " (schema):", columnName + ", " + logDataType.toString() + ", " + baseTableColumnKind.name());
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
        parser.addArgument("-s", "--source").required(true)
                .setDefault("127.0.0.1").help("Address of a node in source cluster");

        try {
            return parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(-1);
            return null;
        }
    }
}
