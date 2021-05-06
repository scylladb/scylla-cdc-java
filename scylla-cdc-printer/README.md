# scylla-cdc-java Printer

## Overview

Printer is an example application that uses the scylla-cdc-java library to print all changes from CDC log for a given table. It is meant as a beginner's introduction to the basic features of the library. See [Do it youself! section](#do-it-yourself) to read more about how to use the library.

## Installation and usage
```bash
git clone https://github.com/scylladb/scylla-cdc-java.git
cd scylla-cdc-java
mvn clean install
cd scylla-cdc-printer
./scylla-cdc-printer -k KEYSPACE -t TABLE -s SOURCE
```

Command-line arguments:
```
usage: ./scylla-cdc-printer [-h] -k KEYSPACE -t TABLE -s SOURCE

named arguments:
  -h, --help             show this help message and exit
  -k KEYSPACE, --keyspace KEYSPACE
                         Keyspace name
  -t TABLE, --table TABLE
                         Table name
  -s SOURCE, --source SOURCE
                         Address of a node in source cluster (default: 127.0.0.1)
  -e, --events
  						 Print CDC batch sequence events
```

## Do it yourself!

### Setting up the CDC consumer
Let's go through the Printer code and learn how to use the library. You can see the final code [here](src/main/java/com/scylladb/cdc/printer/Main.java).

To consume changes, we specify a class which implements `RawChangeConsumer` interface (here by using a lambda). The consumer returns a `CompletableFuture`, so you can react to CDC changes and perform some I/O or longer processing.

```java
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

```

The CDC consumer executed in a `ScheduledExecutorService` in an arbitrary number of threads (controllable by user). The reading will be divided into distinct subsets of the CDC log (partitioned based on Vnodes). Those multiple task sets will cumulatively read the entire CDC log. All changes related to the same row (more generally the same partition key) will appear sequentially, but may occur on different threads, thus a consumer needs to be thread safe. Note that after a topology change (adding or removing nodes from the Scylla cluster) this task set division will be reset.

Next, we create an instance of `Consumer` based on a callback object. This instance is shared by all threads in the executor.

In the printer demo case, we optionally use two different callback types:

1. A `RawChangeConsumer` callback, which is called for each CDC row read (in order per stream group). single consumer shared by all threads. 

```java
Consumer c;

...

RawChangeConsumer rc = change -> {
    // Print the change. See printChange()
    // for more information on how to
    // access its details.
    printChange(change);
    return CompletableFuture.completedFuture(null);
};
c = Consumer.forRawChangeConsumer(rc);

```

2. A `BatchSequenceConsumer` callback, which is called for each CDC batch sequence when fully read. This callback type provides "grouping" of the reported data by the distinct CDC event that caused it. These two are guaranteed to be reported in order within the CDC stream group of the task set (i.e. Vnode), but is still potentially called on many, and different threads and must also be thread safe.

```java
Consumer c;

...

BatchSequenceConsumer bc = e -> {
    printSequence(e);
    return CompletableFuture.completedFuture(null);
};
c = Consumer.forBatchSequenceConsumer(bc);
```

Finally, we can build a `CDCConsumer` instance and start it! When using `CDCConsumer.builder()` you should provide the following configuration:
- Contact points (`addContactPoint()`) used to connect to the Scylla cluster.
- Tables to read (`addTable()`). The provided name should be of a base table, *not* the CDC log tables (e.g. `ks.t` not `ks.t_scylla_cdc_log`).
- Consumer instance (`withConsumer()`) which will receive the CDC changes.

You can stop the `CDCConsumer` by calling the `stop()` method or by constructing the `CDCConsumer` in `try` (try-with-resources), which will stop it after the `try` block.

```java
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

    waitForCtrlC(consumer); // helper
}

// The CDCConsumer is gracefully stopped after try-with-resources.
```

### Consuming CDC changes

Let's implement the `printChange(RawChange change)` method and see what information is available about the change. The `RawChange` object represents a single row of CDC log. First, we get information about the change id: its stream id and time:

```java
private static void printChange(RawChange change) {
    printChange(Type.Row, change, change.getOperationType(), Collections.singleton(change));
}

private static void printChange(Type type, Change change, OperationType optype, Collection<RawChange> rows) {
    // Get the ID of the change which contains stream_id and time.
    ChangeId changeId = change.getId();
    StreamId streamId = changeId.getStreamId();
    ChangeTime changeTime = changeId.getChangeTime();

```

Those accessors correspond to `cdc$stream_id` and `cdc$time` columns.

We can get the [operation type](https://docs.scylladb.com/using-scylla/cdc/cdc-log-table/#operation-column) (if it was an `INSERT`, `UPDATE` etc.):

```java
// Get the operation type, for example: ROW_UPDATE, POST_IMAGE.
RawChange.OperationType operationType = change.getOperationType();
```

In each `RawChange` there is information about the schema of the change - column names, data types, whether the column is part of the primary key:

```java
ChangeSchema changeSchema = change.getSchema();
```

There are two types of columns inside ChangeSchema:
1. [CDC log columns](https://docs.scylladb.com/using-scylla/cdc/cdc-log-table/) (`cdc$time`, `cdc$stream_id`, ...)
2. base table columns

CDC log columns can be easily accessed by `RawChange` helper methods (such as `getTTL()`, `getId()`). Let's concentrate on non-CDC columns (those are from the base table) and iterate over them:


In each `RawChange` there is an information about the change schema - column names, data types, whether the column is part of primary key:

```java
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
Cell cell = change.getCell(columnName);
Object cellValue = cell.getAsObject();
```

If we know the type of a given cell, we can get the value as a specific type:
```java
Integer intCellValue = cell.getInt();
```

You can read the full source code of Printer [here](src/main/java/com/scylladb/cdc/printer/Main.java). 

If you want to learn more about advanced use-cases of the library, see the [Replicator example project](https://github.com/scylladb/scylla-cdc-java/tree/master/scylla-cdc-replicator).
