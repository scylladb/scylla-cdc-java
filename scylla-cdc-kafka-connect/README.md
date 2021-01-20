# Scylla CDC Source Connector

## Overview

Scylla CDC Source Connector is a source connector capturing row-level changes in the tables of a Scylla cluster. It is a Debezium connector, compatible with Kafka Connect (with Kafka 2.6.0+) and built on top of scylla-cdc-java library.

The connector reads the CDC log for selected tables and produces Kafka messages for each row-level `INSERT`, `UPDATE` or `DELETE` operation. The connector is able to split reading the CDC log accross multiple processes: the connector can start a separate Kafka Connect task for reading each [Vnode of Scylla cluster](https://docs.scylladb.com/architecture/ringarchitecture/) allowing for high throughput. You can limit the number of started tasks by using `tasks.max` property.

Scylla CDC Source Connector seamlessly handles schema changes and topology changes (adding, removing nodes from Scylla cluster). The connector is fault-tolerant, retrying reading data from Scylla in case of failure. It periodically saves the current position in Scylla CDC log using Kafka Connect offset tracking (configurable by `offset.flush.interval.ms` parameter). If the connector is stopped, it is able to resume reading from previously saved offset. Scylla CDC Source Connector has at-least-once semantics.

The following features are currently *not* supported:
- Partition deletes - those changes are ignored
- Row range deletes - those changes are ignored
- Collection types (`LIST`, `SET`, `MAP`) and `UDT` - columns with those types are omitted from generated messages
- Authentication and authorization
- Preimage and postimage - changes only contain those columns that were modified, not the entire row before/after change. More information [here](#cell-representation)

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
key.converter.schemas.enable=true
value.converter.schemas.enable=true

heartbeat.interval.ms=30000
auto.create.topics.enable=true
```

This configuration will capture row-level changes in the `ks.my_table` table from Scylla cluster (`127.0.0.1`, `127.0.0.2`). Change data capture events will appear on `MyScyllaCluster_ks_my_table` Kafka topic encoded as JSONs with schema information.

Scylla CDC Source Connector writes events to a separate Kafka topic for each source Scylla table. The topic name will be: `logicalName_keyspaceName_tableName` (logical name configured by `scylla.name` property). You can turn on automatic topic creation by using the `auto.create.topics.enable` property.

### Data change events
Scylla CDC Source Connector generates a data change event for each row-level `INSERT`, `UPDATE` or `DELETE` operation. Each event consists of key and value.

Debezium and Kafka Connect are designed around continuous streams of event messages, and the structure of these events may change over time. This could be difficult for consumers to deal with, so to make it easy Kafka Connect makes each event self-contained. Every message key and value has two parts: a schema and payload. The schema describes the structure of the payload, while the payload contains the actual data.

#### Data change event key
The data change event's key will contain a field for each column in the primary key (partition key and clustering key).

For example, given this Scylla table and `INSERT` operation:
```
CREATE TABLE ks.t(
    pk int, ck int, v text, PRIMARY KEY(pk, ck)
) WITH cdc = {'enabled': true};

INSERT INTO ks.t(pk, ck, v) VALUES (1, 1, 'example row');
```

The data change event's key will look like this (with JSON serializer and schema enabled):
```json
{
    "schema": {
        "type": "struct",
        "fields": [
            {
                "type": "int32",
                "optional": true,
                "field": "ck"
            },
            {
                "type": "int32",
                "optional": true,
                "field": "pk"
            }
        ],
        "optional": false,
        "name": "ks.t.Key"
    },
    "payload": {
        "ck": 1,
        "pk": 1
    }
}
```

#### Data change event value
Data change event's value consists of a schema and a payload section. The payload of every data change events contains the following fields:

- `op`: type of operation. `c` for `INSERT`, `u` for `UPDATE`, `d` for `DELETE`.
- `before`: an optional field with state of the row before the event occurred. Present in `DELETE` data change events.
- `after`: an optional field with state of the row after the event occurred. Present in `UPDATE` and `INSERT` data change events.
- `source`: metadata about the source of event.
- `ts_ms`: time at which connector processed the event.

##### Cell representation
Operations in Scylla, such as `INSERT` or `UPDATE`, do not have to modify all columns of a row. To differentiate between non-modification of column and inserting/updating `NULL`, all non-primary-key columns are wrapped with structure containing a single `value` field. For example, given this Scylla table and `UPDATE` operation:

```
CREATE TABLE ks.t(
    pk int, ck int, v text, PRIMARY KEY(pk, ck)
) WITH cdc = {'enabled': true};

INSERT INTO ks.t(pk, ck, v) VALUES (1, 1, 'example row');
UPDATE ks.t SET v = 'new value' WHERE pk = 1 AND ck = 1;
```

The `v` column will be represented as:
```json
...
    "v": {
        "value": "new value"
    }
...
```

In case of `UPDATE` setting `v` to `NULL`:
```
UPDATE ks.t SET v = NULL WHERE pk = 1 AND ck = 1;
```

The `v` column will be represented as:
```json
...
    "v": {
        "value": null
    }
...
```

If the operation did not modify the `v` column, the data event will contain the following representation of `v`:
```json
...
    "v": null
...
```

See `UPDATE` example for full  data change event's value.

##### `INSERT` example
Given this Scylla table and `INSERT` operation:
```
CREATE TABLE ks.t(
    pk int, ck int, v text, PRIMARY KEY(pk, ck)
) WITH cdc = {'enabled': true};

INSERT INTO ks.t(pk, ck, v) VALUES (1, 1, 'example row');
```

The connector will generate the following data change event's value (with JSON serializer and schema enabled):
```json
{
    "schema": {
        "type": "struct",
        "fields": [
            {
                "type": "struct",
                "fields": [
                    {
                        "type": "string",
                        "optional": false,
                        "field": "version"
                    },
                    {
                        "type": "string",
                        "optional": false,
                        "field": "connector"
                    },
                    {
                        "type": "string",
                        "optional": false,
                        "field": "name"
                    },
                    {
                        "type": "int64",
                        "optional": false,
                        "field": "ts_ms"
                    },
                    {
                        "type": "string",
                        "optional": true,
                        "name": "io.debezium.data.Enum",
                        "version": 1,
                        "parameters": {
                            "allowed": "true,last,false"
                        },
                        "default": "false",
                        "field": "snapshot"
                    },
                    {
                        "type": "string",
                        "optional": false,
                        "field": "db"
                    },
                    {
                        "type": "string",
                        "optional": false,
                        "field": "keyspace_name"
                    },
                    {
                        "type": "string",
                        "optional": false,
                        "field": "table_name"
                    }
                ],
                "optional": false,
                "name": "com.scylladb.cdc.debezium.connector",
                "field": "source"
            },
            {
                "type": "struct",
                "fields": [
                    {
                        "type": "int32",
                        "optional": true,
                        "field": "ck"
                    },
                    {
                        "type": "int32",
                        "optional": true,
                        "field": "pk"
                    },
                    {
                        "type": "struct",
                        "fields": [
                            {
                                "type": "string",
                                "optional": true,
                                "field": "value"
                            }
                        ],
                        "optional": true,
                        "field": "v"
                    }
                ],
                "optional": true,
                "field": "before"
            },
            {
                "type": "struct",
                "fields": [
                    {
                        "type": "int32",
                        "optional": true,
                        "field": "ck"
                    },
                    {
                        "type": "int32",
                        "optional": true,
                        "field": "pk"
                    },
                    {
                        "type": "struct",
                        "fields": [
                            {
                                "type": "string",
                                "optional": true,
                                "field": "value"
                            }
                        ],
                        "optional": true,
                        "field": "v"
                    }
                ],
                "optional": true,
                "field": "after"
            },
            {
                "type": "string",
                "optional": true,
                "field": "op"
            },
            {
                "type": "int64",
                "optional": true,
                "field": "ts_ms"
            },
            {
                "type": "struct",
                "fields": [
                    {
                        "type": "string",
                        "optional": false,
                        "field": "id"
                    },
                    {
                        "type": "int64",
                        "optional": false,
                        "field": "total_order"
                    },
                    {
                        "type": "int64",
                        "optional": false,
                        "field": "data_collection_order"
                    }
                ],
                "optional": true,
                "field": "transaction"
            }
        ],
        "optional": false,
        "name": "ks.t.Envelope"
    },
    "payload": {
        "source": {
            "version": "1.0.0",
            "connector": "scylla",
            "name": "MyScyllaCluster",
            "ts_ms": 1610447874949,
            "snapshot": "false",
            "db": "ks",
            "keyspace_name": "ks",
            "table_name": "t"
        },
        "before": null,
        "after": {
            "ck": 1,
            "pk": 1,
            "v": {
                "value": "example row"
            }
        },
        "op": "c",
        "ts_ms": 1610449737807,
        "transaction": null
    }
}
```

##### `UPDATE` example
Given this Scylla table and `UPDATE` operations:
```
CREATE TABLE ks.t(
    pk int, ck int, v text, PRIMARY KEY(pk, ck)
) WITH cdc = {'enabled': true};

UPDATE ks.t SET v = 'new value' WHERE pk = 1 AND ck = 1;
UPDATE ks.t SET v = NULL WHERE pk = 1 AND ck = 1;
```

The connector will generate the following data change event's value (with JSON serializer and schema enabled) for the first `UPDATE`. Note that `schema` is ommitted as it is the same as in `INSERT` example:
```json
{
  "schema": {},
  "payload": {
    "source": {
      "version": "1.0.0",
      "connector": "scylla",
      "name": "MyScyllaCluster",
      "ts_ms": 1610451234949,
      "snapshot": "false",
      "db": "ks",
      "keyspace_name": "ks",
      "table_name": "t"
    },
    "before": null,
    "after": {
      "ck": 1,
      "pk": 1,
      "v": {
        "value": "new value"
      }
    },
    "op": "u",
    "ts_ms": 1610451295022,
    "transaction": null
  }
}
```

Data change event's value for the second `UPDATE`:
```json
{
  "schema": {},
  "payload": {
    "source": {
      "version": "1.0.0",
      "connector": "scylla",
      "name": "MyScyllaCluster",
      "ts_ms": 1610451324949,
      "snapshot": "false",
      "db": "ks",
      "keyspace_name": "ks",
      "table_name": "t"
    },
    "before": null,
    "after": {
      "ck": 1,
      "pk": 1,
      "v": {
        "value": null
      }
    },
    "op": "u",
    "ts_ms": 1610451385009,
    "transaction": null
  }
}
```

##### `DELETE` example
Given this Scylla table and `DELETE` operation:
```
CREATE TABLE ks.t(
    pk int, ck int, v text, PRIMARY KEY(pk, ck)
) WITH cdc = {'enabled': true};

DELETE FROM ks.t WHERE pk = 1 AND ck = 1;
```

The connector will generate the following data change event's value (with JSON serializer and schema enabled). Note that `schema` is ommitted as it is the same as in `INSERT` example:
```json
{
  "schema": {},
  "payload": {
    "source": {
      "version": "1.0.0",
      "connector": "scylla",
      "name": "MyScyllaCluster",
      "ts_ms": 1610451834949,
      "snapshot": "false",
      "db": "ks",
      "keyspace_name": "ks",
      "table_name": "t"
    },
    "before": {
      "ck": 1,
      "pk": 1,
      "v": null
    },
    "after": null,
    "op": "d",
    "ts_ms": 1610451894968,
    "transaction": null
  }
}
```
