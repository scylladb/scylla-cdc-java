package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.scylladb.cdc.cql.CQLConfiguration;

public class Driver3Session implements AutoCloseable {
    private final Cluster driverCluster;
    private final Session driverSession;

    public Driver3Session(CQLConfiguration cqlConfiguration) {
        Cluster.Builder clusterBuilder = Cluster.builder()
                .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED);

        clusterBuilder = clusterBuilder.addContactPointsWithPorts(cqlConfiguration.contactPoints);

        String user = cqlConfiguration.user, password = cqlConfiguration.password;
        if (user != null && password != null) {
            clusterBuilder = clusterBuilder.withCredentials(user, password);
        }

        driverCluster = clusterBuilder.build();
        driverSession = driverCluster.connect();
    }

    protected Session getDriverSession() {
        return driverSession;
    }

    @Override
    public void close() {
        if (driverSession != null) {
            driverSession.close();
        }
        if (driverCluster != null) {
            driverCluster.close();
        }
    }
}
