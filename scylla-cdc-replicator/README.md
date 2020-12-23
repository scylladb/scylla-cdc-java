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
usage: ./scylla-cdc-replicator [-h] [-m MODE] -k KEYSPACE -t TABLE -s SOURCE -d DESTINATION [-cl CONSISTENCY_LEVEL]

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
                         Consistency level of writes. QUORUM by default (default: quorum)
```

## Implementation overview

Replicator's [`Main`](src/main/java/com/scylladb/cdc/replicator/Main.java) class reads the command-line arguments and starts up `CDCConsumer` with [`ReplicatorConsumer`](src/main/java/com/scylladb/cdc/replicator/ReplicatorConsumer.java).

[`ReplicatorConsumer`](src/main/java/com/scylladb/cdc/replicator/ReplicatorConsumer.java) consumes the CDC log changes provided by `CDCConsumer`. Depending on the change type (`ROW_INSERT`, `ROW_UPDATE`, etc.) it executes a handler for a change type. Those handlers translate the change to a CQL query and perform it on a destination cluster.

