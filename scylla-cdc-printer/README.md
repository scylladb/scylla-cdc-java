# scylla-cdc-java Printer

## Overview

Printer is an example application that uses the scylla-cdc-java library to print all changes from CDC log for a given table.

## Do it yourself!

Let's go through the Printer code and learn how to use the library. You can see the final code [here](src/main/java/com/scylladb/cdc/printer/Main.java).

First, we establish a connection to the Scylla cluster using the Scylla Java Driver in version 3.x:

```java
// Build the connection to the cluster. Currently, the library supports
// only the Scylla Java Driver in version 3.x.
try (Cluster cluster = Cluster.builder().addContactPoint(source).build();
     Session session = cluster.connect()) {
```

Having established a connection, we have to specify which tables CDC log we want to read. The provided name should be of a base table, *not* the CDC log tables (e.g. `ks.t` not `ks.t_scylla_cdc_log`):

```java
// First, create a set of tables we want to watch.
// They should have CDC enabled on them. Provide
// the base table name, e.g. ks.t; NOT ks.t_scylla_cdc_log.
Set<TableName> tables = Collections.singleton(new TableName(keyspace, table));
```

To consume changes, we specify a class which implements `RawChangeConsumer` interface (here by using a lambda). The consumer returns a `CompletableFuture`, so you can react to CDC changes and perform some I/O or longer processing.

```java
RawChangeConsumer changeConsumer = change -> {
    // Print the change. See printChange()
    // for more information on how to
    // access its details.
    printChange(change);
    return CompletableFuture.completedFuture(null);
};
```

As the CDC consumer can be started multi-threaded, we specify a `RawChangeConsumerProvider` which builds a `RawChangeConsumer` for each thread. If your consumer is thread-safe, your consumer provider should return the same consumer. In case your consumer if not thread-safe, your provider should build a separate consumer for each thread (as below):

```java
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
```

Finally, we can build a `CDCConsumer` instance and start it! If we are finished consuming the changes, we should call a `stop()` method.

```java
// Build a CDCConsumer instance. We start it in a single-thread
// configuration: workersCount(1).
CDCConsumer consumer = CDCConsumerBuilder.builder(session, changeConsumerProvider, tables).workersCount(1).build();

// Start a consumer. You can stop it by using .stop() method.
// In waitForConsumerStop() we are waiting for SIGINT
// and then calling the stop() method.
consumer.start();
waitForConsumerStop(consumer);
```

Let's look into the `printChange(RawChange change)` method and see what information is available about the change. First, there is information about the change id: its stream id and time:

```java
private static void printChange(RawChange change) {
    // Get the ID of the change which contains stream_id and time.
    ChangeId changeId = change.getId();
    StreamId streamId = changeId.getStreamId();
    ChangeTime changeTime = changeId.getChangeTime();
```

Those accessors correspond to `cdc$stream_id` and `cdc$time` columns.

We can get the operation type (if it was an `INSERT`, `UPDATE` etc.):

```java
// Get the operation type, for example: ROW_UPDATE, POST_IMAGE.
RawChange.OperationType operationType = change.getOperationType();
```

In each `RawChange` there is an information about the change schema - column names, data types, whether the column is part of primary key:

```java
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

for (ChangeSchema.ColumnDefinition columnDefinition : nonCdcColumnDefinitions) {
    String columnName = columnDefinition.getColumnName();

    // We can get information if this column was a part of primary key
    // in the base table. Note that in CDC log table different columns
    // are part of primary key (cdc$stream_id, cdc$time, batch_seq_no).
    ChangeSchema.ColumnType baseTableColumnType = columnDefinition.getBaseTableColumnType();

    // Get the information about the data type (as present in CDC log).
    ChangeSchema.DataType logDataType = columnDefinition.getCdcLogDataType();
```

We can also read the value of a given cell (column) in the change:
```java
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
```

You can read the full source code of Printer [here](src/main/java/com/scylladb/cdc/printer/Main.java).
