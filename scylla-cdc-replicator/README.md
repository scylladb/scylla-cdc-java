# scylla-cdc-java Replicator

## Overview

Replicator is an example application that replicates a table from one Scylla cluster to another using the CDC log and scylla-cdc-java library.

## Installation and usage
```bash
git clone https://github.com/scylladb/scylla-cdc-java.git
cd scylla-cdc-java
mvn clean install
cd scylla-cdc-replicator
./scylla-cdc-replicator -k KEYSPACE -t TABLE -s SOURCE -d DESTINATION
```

Command-line arguments:
```
usage: ./scylla-cdc-replicator [-h] [-m MODE] -k KEYSPACE -t TABLE -s SOURCE -d DESTINATION
                             [-cl CONSISTENCY_LEVEL] [--source-read-consistency SOURCE_READ_CONSISTENCY]
                             [--source-datacenter SOURCE_DATACENTER]

named arguments:
  -h, --help             show this help message and exit
  -m MODE, --mode MODE   Mode of operation. Can be delta, preimage or postimage. Default is delta (default: delta)
  -k KEYSPACE, --keyspace KEYSPACE
                         Keyspace name
  -t TABLE, --table TABLE
                         Table names, provided as a comma delimited string
  -s SOURCE, --source SOURCE
                         Address of a node in source cluster
  -d DESTINATION, --destination DESTINATION
                         Address of a node in destination cluster
  -cl CONSISTENCY_LEVEL, --consistency-level CONSISTENCY_LEVEL
                         Consistency level of writes. LOCAL_QUORUM by default (default: local_quorum)
  --source-read-consistency SOURCE_READ_CONSISTENCY
                         Consistency level of source CDC reads. Uses the library default (LOCAL_QUORUM) when omitted
  --source-datacenter SOURCE_DATACENTER
                         Datacenter for source CDC queries. Query routing can use any datacenter when omitted
```

The source options affect CDC reads; `-cl` controls destination writes independently.
For a source receiving `LOCAL_QUORUM` base-table writes, set `--source-datacenter`
to the same datacenter as those write coordinators so source CDC reads overlap the
acknowledged writes. For example:

```bash
./scylla-cdc-replicator -k KEYSPACE -t TABLE -s SOURCE -d DESTINATION --source-datacenter dc1
```

For source writes acknowledged at global `QUORUM`, use
`--source-read-consistency quorum` to retain global write/read quorum overlap.
Without a source datacenter, query routing can select any datacenter; the default
`LOCAL_QUORUM` reads then do not guarantee overlap with writes in another datacenter.
The source datacenter option restricts routing rather than providing remote-datacenter
fallback. Switching to global `QUORUM` can require replicas across datacenters.
These options leave the independent generation/topology consistency policy unchanged.

You can adjust the logging level of Replicator by editing [`src/main/resources/logging.properties`](src/main/resources/logging.properties) file, when launching with `./scylla-cdc-replicator` script. If you start the replicator externally, use `-Djava.util.logging.config.file=` JVM system property to specify a path to `logging.properties` file.

## Implementation overview

Replicator's [`Main`](src/main/java/com/scylladb/cdc/replicator/Main.java) class reads the command-line arguments and starts up `CDCConsumer` with [`ReplicatorConsumer`](src/main/java/com/scylladb/cdc/replicator/ReplicatorConsumer.java).

[`ReplicatorConsumer`](src/main/java/com/scylladb/cdc/replicator/ReplicatorConsumer.java) consumes the CDC log changes provided by `CDCConsumer`. Depending on the change type (`ROW_INSERT`, `ROW_UPDATE`, etc.) it executes a handler for a change type. Those handlers translate the change to a CQL query and perform it on a destination cluster.

