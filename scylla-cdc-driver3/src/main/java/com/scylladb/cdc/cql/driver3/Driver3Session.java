package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.scylladb.cdc.cql.CQLConfiguration;

public class Driver3Session implements AutoCloseable {
    private final Cluster driverCluster;
    private final Session driverSession;
    private final ConsistencyLevel consistencyLevel;

    public Driver3Session(CQLConfiguration cqlConfiguration) {
        Cluster.Builder clusterBuilder = Cluster.builder()
                .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED);

        clusterBuilder = clusterBuilder.addContactPointsWithPorts(cqlConfiguration.contactPoints);

        String user = cqlConfiguration.user, password = cqlConfiguration.password;
        if (user != null && password != null) {
            clusterBuilder = clusterBuilder.withCredentials(user, password);
        }

        if (cqlConfiguration.getLocalDCName() != null) {
            clusterBuilder = clusterBuilder.withLoadBalancingPolicy(
                    DCAwareRoundRobinPolicy.builder().withLocalDc(cqlConfiguration.getLocalDCName()).build());
        }

        driverCluster = clusterBuilder.build();
        driverSession = driverCluster.connect();

        switch (cqlConfiguration.getConsistencyLevel()) {
            case ONE:
                consistencyLevel = ConsistencyLevel.ONE;
                break;
            case TWO:
                consistencyLevel = ConsistencyLevel.TWO;
                break;
            case THREE:
                consistencyLevel = ConsistencyLevel.THREE;
                break;
            case QUORUM:
                consistencyLevel = ConsistencyLevel.QUORUM;
                break;
            case ALL:
                consistencyLevel = ConsistencyLevel.ALL;
                break;
            case LOCAL_QUORUM:
                consistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
                break;
            case LOCAL_ONE:
                consistencyLevel = ConsistencyLevel.LOCAL_ONE;
                break;
            default:
                throw new IllegalStateException("Unsupported consistency level: " + cqlConfiguration.getConsistencyLevel());
        }
    }

    protected Session getDriverSession() {
        return driverSession;
    }

    protected ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
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
