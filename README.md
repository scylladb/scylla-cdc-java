# scylla-cdc-java
[![Tests](https://img.shields.io/github/workflow/status/scylladb/scylla-cdc-java/Tests/master?label=Tests)](https://github.com/scylladb/scylla-cdc-java/actions/workflows/tests.yml) [![Release](https://img.shields.io/maven-central/v/com.scylladb/scylla-cdc-base?label=Release)](https://search.maven.org/search?q=g:com.scylladb%20AND%20a:scylla-cdc*) 

scylla-cdc-java is a library that makes it easy to develop Java applications consuming the [Scylla CDC log](https://docs.scylladb.com/using-scylla/cdc/). The library automatically and transparently handles errors and topology changes of the underlying Scylla cluster. It provides a simple API for reading the CDC log, as well as examples and ready-made tools, such as replicator.

It is recommended to get familiar with the Scylla CDC documentation first, in order to understand the concepts used in the documentation of scylla-cdc-java: https://docs.scylladb.com/using-scylla/cdc/.

The repository contains two example applications that use the library:
- [Printer](scylla-cdc-printer): connects to the Scylla cluster and prints all changes from CDC log for a selected table.
- [Replicator](scylla-cdc-replicator): replicates a table from a source Scylla cluster to a destination Scylla cluster by reading the CDC log.

## Scylla CDC Source Connector
[Scylla CDC Source Connector](https://github.com/scylladb/scylla-cdc-source-connector) is a source connector capturing row-level changes in the tables of a Scylla cluster. It is a Debezium connector, compatible with Kafka Connect (with Kafka 2.6.0+) and built on top of scylla-cdc-java library.

Read [here](https://github.com/scylladb/scylla-cdc-source-connector) more about the Connector and how to install and configure it.

## Why Use a Library?
Scylla's design of CDC is based on the concept of CDC log tables. For every table whose changes you wish to track, an associated CDC log table is created. We refer to this new table as the CDC log table and the original table as a base table. Every time you modify your data in the base table — insert, update or delete — this fact is recorded by inserting one or more rows to the corresponding CDC log table.

This approach makes it possible to use tools that already exist in order to read from a CDC log. Everything is accessible through CQL and the schema of CDC log tables is documented by us, so it's possible to write an application consuming CDC with the help of a driver (or even `cqlsh`).

However, the CDC log format is more complicated than a single queue of events. You need to know the design of Scylla CDC well in order to implement an application that is performant and robust. Fortunately, the `scylla-cdc-java` library will handle those concerns for you. You can use its convenient API so that you can concentrate on writing the business logic of your application.

## Installation

The latest release of scylla-cdc-java is available on Maven Central. You can integrate it in your application by using the following Maven dependencies:
```xml
<dependency>
    <groupId>com.scylladb</groupId>
    <artifactId>scylla-cdc-lib</artifactId>
    <version>1.1.0</version>
</dependency>
```

You can also build the library from source by using the following commands:
```bash
git clone https://github.com/scylladb/scylla-cdc-java.git
cd scylla-cdc-java
mvn clean install
```

By default, during the installation, a suite of Docker integration tests are run. If you wish to disable them, pass a
`-DskipITs` flag: `mvn clean install -DskipITs`.

## Getting started

The following code snippet establishes a connection to local Scylla cluster (`127.0.0.1`) and starts printing CDC log rows from CDC table of `ks.table`.

```java
RawChangeConsumerProvider changeConsumerProvider = threadId -> {
    RawChangeConsumer changeConsumer = change -> {
        System.out.println(change);
        return CompletableFuture.completedFuture(null);
    };
    return changeConsumer;
};

try (CDCConsumer consumer = CDCConsumer.builder()
        .addContactPoint("127.0.0.1")
        .addTable(new TableName("ks", "table"))
        .withConsumerProvider(changeConsumerProvider)
        .withWorkersCount(1).build()) {
    consumer.start();
    Thread.sleep(10000);
} catch (InterruptedException e) {
    e.printStackTrace();
}
```

The consumer is started as a single-thread CDC consumer and reads the CDC log for 10 seconds.

**Next steps: read more about how to use the library in the [Printer example application documentation](scylla-cdc-printer).**

## Useful links

- [Scylla Docs - Change Data Capture (CDC)](https://docs.scylladb.com/using-scylla/cdc/)
- [Scylla University - Change Data Capture (CDC)](https://university.scylladb.com/courses/scylla-operations/lessons/change-data-capture-cdc/)
- [ScyllaDB YouTube - Change Data Capture in Scylla](https://www.youtube.com/watch?v=392Nbfrq7Dg)
- [ScyllaDB Blog - Consuming CDC with Java and Go](https://www.scylladb.com/2021/02/09/consuming-cdc-with-java-and-go/)
- [ScyllaDB Blog - Using Change Data Capture (CDC) in Scylla](https://www.scylladb.com/2020/07/23/using-change-data-capture-cdc-in-scylla/)
- [scylla-cdc-go - A library for Go](https://github.com/scylladb/scylla-cdc-go)

## Contact

Use the [GitHub Issues](https://github.com/scylladb/scylla-cdc-java/issues) to report bugs or errors. You can also join [ScyllaDB-Users Slack channel](http://slack.scylladb.com/) and discuss on `#cdc` channel.

## License

The library is licensed under Apache License 2.0. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 or in the LICENSE.txt file in the repository.
