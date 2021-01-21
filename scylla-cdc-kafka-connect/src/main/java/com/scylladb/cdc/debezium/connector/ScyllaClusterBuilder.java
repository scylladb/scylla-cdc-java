package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;

import java.net.InetSocketAddress;
import java.util.List;

public class ScyllaClusterBuilder {
    private final ScyllaConnectorConfig configuration;

    public ScyllaClusterBuilder(ScyllaConnectorConfig configuration) {
        this.configuration = configuration;
    }

    public Cluster build() {
        Cluster.Builder clusterBuilder = Cluster.builder()
                .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED);

        List<InetSocketAddress> contactPoints = configuration.getContactPoints();
        clusterBuilder = clusterBuilder.addContactPointsWithPorts(contactPoints);

        String user = configuration.getUser(), password = configuration.getPassword();
        if (user != null && password != null) {
            clusterBuilder = clusterBuilder.withCredentials(user, password);
        }

        return clusterBuilder.build();
    }
}
