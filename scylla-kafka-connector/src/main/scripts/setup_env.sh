#!/bin/bash
# These environment variables need to be setup
# Go to folder where files exist.
# shellcheck disable=SC2164

cd /scylla-kafka/

export FILE=scylla_config.yml

export SCYLLA_PROD_HOST=$(echo -ne "$SCYLLA_PROD_HOST")
# shellcheck disable=SC2155
export SCYLLA_PROD_PORT=$(echo -ne $SCYLLA_PROD_PORT)
# shellcheck disable=SC2155
export SCYLLA_PROD_USERNAME=$(echo -ne "$SCYLLA_PROD_USERNAME")
# shellcheck disable=SC2155
export SCYLLA_PROD_PASSWORD=$(echo -ne "$SCYLLA_PROD_PASSWORD")
# shellcheck disable=SC2155
export SCYLLA_PROD_REGION=$(echo -ne "$SCYLLA_PROD_REGION")
# shellcheck disable=SC2155
export KAFKA_ENDPOINT_WITH_PORT=$(echo -ne "$KAFKA_ENDPOINT_WITH_PORT")

export ORG=$(echo -ne "$ORG")
# shellcheck disable=SC2155
export TENANT=$(echo -ne "$TENANT")
export KEYSPACE_AND_TABLES=$(echo -ne "$KEYSPACE_AND_TABLES")

export XMS=$(echo -ne "$XMS")
export XMX=$(echo -ne "$XMX")
export WORKER_COUNT=$(echo -ne "$WORKER_COUNT")
export CHECKPOINT_AFTER_ROWS=$(echo -ne "$CHECKPOINT_AFTER_ROWS")

export SLACK_WEBHOOK_URL=$(echo -ne "$SLACK_WEBHOOK_URL")

export MYSQL_PWD=$(echo -ne "$MYSQL_PWD")
# shellcheck disable=SC2155
export MYSQL_USR=$(echo -ne "$MYSQL_USR")
# shellcheck disable=SC2155
export MYSQL_HOST_SYCLLA_CONNECTOR=$(echo -ne "$MYSQL_HOST_SYCLLA_CONNECTOR")

sed -i -e 's#org:.*#org: '"$ORG"'#' "$FILE"
sed -i -e 's#tenant:.*#tenant: '"$TENANT"'#' "$FILE"
sed -i -e 's#keySpacesAndTablesList:.*#keySpacesAndTablesList: '"$KEYSPACE_AND_TABLES"'#' "$FILE"
sed -i -e 's#workersCount:.*#workersCount: '"$WORKER_COUNT"'#' "$FILE"
sed -i -e 's#checkPointAfterRows:.*#checkPointAfterRows: '"$CHECKPOINT_AFTER_ROWS"'#' "$FILE"
sed -i -e 's#slackWebhookURL:.*#slackWebhookURL: '"$SLACK_WEBHOOK_URL"'#' "$FILE"

sed -i -e 's#host:.*#host: '"$SCYLLA_PROD_HOST"'#' "$FILE"
sed -i -e 's#userName:.*#userName: '"$SCYLLA_PROD_USERNAME"'#' "$FILE"
sed -i -e 's#port:.*#port: '$SCYLLA_PROD_PORT'#' "$FILE"
sed -i -e 's#scPassword:.*#scPassword: '"$SCYLLA_PROD_PASSWORD"'#' "$FILE"

sed -i -e 's#username:.*#username: '"$MYSQL_USR"'#' "$FILE"
sed -i -e 's#password:.*#password: '"$MYSQL_PWD"'#' "$FILE"
sed -i -e 's#jdbcUrl:.*#jdbcUrl: '"$MYSQL_HOST_SYCLLA_CONNECTOR"'#' "$FILE"
sed -i -e 's#region:.*#region: '"$SCYLLA_PROD_REGION"'#' "$FILE"
sed -i -e 's#kafkaBrokers:.*#kafkaBrokers: '"$KAFKA_ENDPOINT_WITH_PORT"'#' "$FILE"





