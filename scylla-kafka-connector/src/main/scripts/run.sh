#!/usr/bin/env bash

set -e
source setup_env.sh

cat /scylla-kafka/scylla_config.yml

exec java $XMS $XMX -jar /scylla-kafka/scylla-kafka-connector.jar --file /scylla-kafka/scylla_config.yml


