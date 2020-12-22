# scylla-cdc-java Replicator

## Overview

Replicator is an example application that replicates a table from one Scylla cluster to another using the CDC log and scylla-cdc-java library.

## Implementation overview

Replicator's [`Main`](src/main/java/com/scylladb/cdc/replicator/Main.java) class reads the command-line arguments and starts up `CDCConsumer` with [`ReplicatorConsumer`](src/main/java/com/scylladb/cdc/replicator/ReplicatorConsumer.java).

[`ReplicatorConsumer`](src/main/java/com/scylladb/cdc/replicator/ReplicatorConsumer.java) consumes the CDC log changes provided by `CDCConsumer`. Depending on the change type (`ROW_INSERT`, `ROW_UPDATE`, etc.) it executes a handler for a change type. Those handlers translate the change to a CQL query and perform it on a destination cluster.


