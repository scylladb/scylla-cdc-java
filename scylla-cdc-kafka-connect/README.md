# Scylla CDC Source Connector

## Overview

Scylla CDC Source Connector is a source connector capturing row-level changes in the tables of a Scylla cluster. It is a Debezium connector, compatible with Kafka Connect (with Kafka 2.6.0+) and built on top of scylla-cdc-java library.

The connector reads the CDC log for selected tables and produces Kafka messages for each row-level `INSERT`, `UPDATE` or `DELETE` operation. The connector is able to split reading the CDC log accross multiple processes: the connector can start a separate Kafka Connect task for reading each [Vnode of Scylla cluster](https://docs.scylladb.com/architecture/ringarchitecture/) allowing for high throughput. You can limit the number of started tasks by using `tasks.max` property.

Scylla CDC Source Connector seamlessly handles schema changes and topology changes (adding, removing nodes from Scylla cluster). The connector is fault-tolerant, retrying reading data from Scylla in case of failure. It periodically saves the current position in Scylla CDC log using Kafka Connect offset tracking (configurable by `offset.flush.interval.ms` parameter). If the connector is stopped, it is able to resume reading from previously saved offset. Scylla CDC Source Connector has at-least-once semantics.

## Connector installation

### Building 

#### Building from source

You can also build the connector from source by using the following commands:
```bash
git clone https://github.com/scylladb/scylla-cdc-java.git
cd scylla-cdc-java
mvn clean package
```

The connector JAR file will be available in `scylla-cdc-kafka-connect/target/fat-jar` directory.

### Installation

Copy the JAR file with connector into your Kafka Connect deployment and append the directory containing the connector to your Kafka Connect's plugin path (`plugin.path` configuration property).

### Configuration

Scylla CDC Source Connector exposes the following configuration properties:

`scylla.name` is as a logical name of Scylla cluster. It is for example used as a Kafka topic prefix, among others. The logical name allows you to easily differentiate between your different Scylla cluster deployments. It should consist of alphanumeric or underscore (`_`) characters. 

`scylla.cluster.ip.addresses` is a comma-delimited list of source Scylla cluster endpoints (`host1:port1,host2:port2`).

`scylla.table.names` is a comma-delimited list of source Scylla tables for connector to consume. Those tables should have Scylla Change Data Capture (CDC) enabled on them. See [Change Data Capture (CDC)](https://docs.scylladb.com/using-scylla/cdc/) for more information about configuring CDC on Scylla.
 
Moreover, you should set `heartbeat.interval.ms` to a positive number (suggested value: 30000). The connector uses heartbeat messages internally to facilitate changing [the current generation](https://docs.scylladb.com/using-scylla/cdc/cdc-stream-generations/), which happens when there is a schema change in the Scylla cluster.
 
Example configuration (as `.properties` file):
```
name=ScyllaCDCSourceConnector
connector.class=com.scylladb.cdc.debezium.connector.ScyllaConnector
scylla.name=MyScyllaCluster
scylla.cluster.ip.addresses=127.0.0.1:9042,127.0.0.2:9042
scylla.table.names=ks.my_table

tasks.max=10
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter

heartbeat.interval.ms=30000
auto.create.topics.enable=true
```

This configuration will capture row-level changes in the `ks.my_table` table from Scylla cluster (`127.0.0.1`, `127.0.0.2`). Change data capture events will appear on `MyScyllaCluster_ks_my_table` Kafka topic encoded as JSONs.

Scylla CDC Source Connector writes events to a separate Kafka topic for each source Scylla table. The topic name will be: `logicalName_keyspaceName_tableName` (logical name configured by `scylla.name` property). You can turn on automatic topic creation by using the `auto.create.topics.enable` property.
