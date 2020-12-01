package com.scylladb.cdc.debezium.connector;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.heartbeat.Heartbeat;
import jdk.jfr.consumer.RecordedObject;
import org.apache.kafka.common.config.ConfigDef;

import java.net.InetSocketAddress;
import java.util.List;

public class ScyllaConnectorConfig extends CommonConnectorConfig {

    public static final Field WORKER_CONFIG = Field.create("scylla.worker.config")
            .withDisplayName("Worker config")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH);

    public static final Field LOGICAL_NAME = Field.create("scylla.name")
            .withDisplayName("Namespace")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Unique name that identifies the Scylla cluster");

    public static final Field CLUSTER_IP_ADDRESSES = Field.create("scylla.cluster.ip.addresses")
            .withDisplayName("List of IP addresses of some nodes in the Scylla cluster that the connector" +
                    "will use to open initial connections to the cluster. Provided as a comma-separated list of pairs <IP>:<PORT>")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("List of IP addresses of some nodes in the Scylla cluster");

    public static final Field TABLE_NAMES = Field.create("scylla.table.names")
            .withDisplayName("List of CDC-enabled table names for connector to read. " +
                    "Provided as a comma-separated list of pairs <keyspace name>.<table name>")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("List of table names to read");

    private static final ConfigDefinition CONFIG_DEFINITION =
            CommonConnectorConfig.CONFIG_DEFINITION.edit()
                    .name("Scylla")
                    .type(WORKER_CONFIG, LOGICAL_NAME, CLUSTER_IP_ADDRESSES)
                    .events(TABLE_NAMES)
                    .create();

    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    private final Configuration config;

    protected ScyllaConnectorConfig(Configuration config) {
        super(config, config.getString(LOGICAL_NAME), 0);
        this.config = config;
    }

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    public List<InetSocketAddress> getContactPoints() {
        return ConfigSerializerUtil.deserializeClusterIpAddresses(config.getString(ScyllaConnectorConfig.CLUSTER_IP_ADDRESSES));
    }

    @Override
    public String getContextName() {
        return "Scylla";
    }

    @Override
    public String getConnectorName() {
        return "scylla";
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        return new ScyllaSourceInfoStructMaker("scylla", Module.version(), this);
    }
}
