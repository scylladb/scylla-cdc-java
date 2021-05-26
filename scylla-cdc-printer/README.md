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
```

## Do it yourself!

### Setting up the CDC consumer
Let's go through the Printer code and learn how to use the library. You can see the final code [here](src/main/java/com/scylladb/cdc/printer/Main.java).

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

The CDC consumer is started multi-threaded, with a configurable number of threads. Each thread will read a distinct subset of the CDC log (partitioned based on Vnodes). Those multiple threads will cumulatively read the entire CDC log. All changes related to the same row (more generally the same partition key) will appear on the same thread. Note that after a topology change (adding or removing nodes from the Scylla cluster) this mapping will be reset.

Next, we create an instance of `RawChangeConsumerProvider` which returns a `RawChangeConsumer` for each thread. We could write the provider in two ways:

1. A single consumer shared by all threads. With such a provider, a single consumer will receive rows read from all worker threads that read the CDC log. Note that the consumer should be thread-safe. Below is an example of such a provider:
```java
// Build a shared consumer of changes.
RawChangeConsumer sharedChangeConsumer = change -> {
    // Print the change. 
    printChange(change);
    return CompletableFuture.completedFuture(null);
};

// Build a provider of this shared consumer. 
RawChangeConsumerProvider changeConsumerProvider = threadId -> {
    return sharedChangeConsumer;
};
```

2. Separate consumer for each thread. With such a provider, a separate consumer will be created for each worker thread. Those multiple consumers will cumulatively read the entire CDC log. Because each consumer receives changes from a single worker thread, they don’t have to be thread-safe. Note that after the topology change (adding or removing a node from the Scylla cluster), consumers are recreated. Below is an example of such a provider:
```java
// Build a provider of consumers. 
RawChangeConsumerProvider changeConsumerProvider = threadId -> {
    // Build a consumer of changes.
    RawChangeConsumer changeConsumer = change -> {
        // Print the change. 
        printChange(change);
        return CompletableFuture.completedFuture(null);
    };
    return changeConsumer;
};
```

Finally, we can build a `CDCConsumer` instance and start it! When using `CDCConsumer.builder()` you should provide the following configuration:
- Contact points (`addContactPoint()`) used to connect to the Scylla cluster.
- Tables to read (`addTable()`). The provided name should be of a base table, *not* the CDC log tables (e.g. `ks.t` not `ks.t_scylla_cdc_log`).
- Consumer provider (`withConsumerProvider()`) which will receive the CDC changes.

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

    // Wait for SIGINT (blocking wait)
    CountDownLatch terminationLatch = new CountDownLatch(1);
    Signal.handle(new Signal("INT"), signal -> terminationLatch.countDown());
    terminationLatch.await();
}

// The CDCConsumer is gracefully stopped after try-with-resources.
```

### Consuming CDC changes

Let's implement the `printChange(RawChange change)` method and see what information is available about the change. The `RawChange` object represents a single row of CDC log. First, we get information about the change id: its stream id and time:

```java
private static void printChange(RawChange change) {
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
    ChangeSchema.ColumnKind baseTableColumnKind = columnDefinition.getBaseTableColumnKind();

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
