# scylla-cdc-java

## Overview

scylla-cdc-java is a library that makes it easy to develop Java applications consuming the [Scylla CDC log](https://docs.scylladb.com/using-scylla/cdc/). The library automatically and transparently handles errors and topology changes of the underlying Scylla cluster. It provides a simple API for reading the CDC log, as well as examples and ready-made tools, such as replicator.

It is recommended to get familiar with the Scylla CDC documentation first, in order to understand the concepts used in the documentation of scylla-cdc-java: https://docs.scylladb.com/using-scylla/cdc/.

The repository contains two example applications that use the library:
- [Printer](scylla-cdc-printer): connects to the Scylla cluster and prints all changes from CDC log for a selected table.
- [Replicator](scylla-cdc-replicator): replicates a table from a source Scylla cluster to a destination Scylla cluster by reading the CDC log.

## Getting started

The following code snippet establishes a connection to local Scylla cluster (`127.0.0.1`) and starts printing CDC log rows from CDC table of `ks.table`.

```java
try (Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
     Session session = cluster.connect()) {
    Set<TableName> tables = Collections.singleton(new TableName("ks", "table"));

    RawChangeConsumerProvider changeConsumerProvider = threadId -> {
        RawChangeConsumer changeConsumer = change -> {
            System.out.println(change);
            return CompletableFuture.completedFuture(null);
        };
        return changeConsumer;
    };

    CDCConsumer consumer = CDCConsumerBuilder.builder(session, changeConsumerProvider, tables)
        .workersCount(1).build();

    consumer.start();
    Thread.sleep(10000);
    consumer.stop();
} catch (InterruptedException ex) {
    ex.printStackTrace();
}
```

The consumer is started as a single-thread CDC consumer and reads the CDC log for 10 seconds.

**Next steps: you can see the entire code of this example and read more about how to use the library in the [Printer example application](scylla-cdc-printer).**
