package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.cql.CQLConfiguration;

public class Driver3Session implements AutoCloseable {
    private final Cluster driverCluster;
    private final Session driverSession;
    private final ConsistencyLevel consistencyLevel;

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    public Driver3Session(CQLConfiguration cqlConfiguration) {
        PoolingOptions poolingOptions = new PoolingOptions()
            .setCoreConnectionsPerHost(
                HostDistance.LOCAL, 1)
            .setMaxConnectionsPerHost(HostDistance.LOCAL, 1)
            .setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)
            .setNewConnectionThreshold(HostDistance.LOCAL, 30000)
            .setCoreConnectionsPerHost(HostDistance.REMOTE, 1)
            .setMaxConnectionsPerHost(HostDistance.REMOTE, 1)
            .setMaxRequestsPerConnection(HostDistance.REMOTE, 2048)
            .setNewConnectionThreshold(HostDistance.REMOTE, 400)
            .setMaxQueueSize(256)
            .setHeartbeatIntervalSeconds(60)
            .setPoolTimeoutMillis(30000);

        Cluster.Builder clusterBuilder = Cluster.builder()
                .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED)
                .withPoolingOptions(poolingOptions)
                .withQueryOptions(new QueryOptions().setMetadataEnabled(true))
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE).withSocketOptions(new SocketOptions().setKeepAlive(true));

        clusterBuilder = clusterBuilder.addContactPointsWithPorts(cqlConfiguration.contactPoints);

        // Deliberately set the protocol version to V4,
        // as V5 implements returning a metadata id (schema id)
        // per each page. Our implementation of Driver3WorkerCQL
        // relies on the fact that the metadata will not change
        // during a single PreparedStatement.
        //
        // See Driver3WorkerCQL, Driver3SchemaFactory,
        // Driver3WorkerCQLIT#testPreparedStatementSameSchemaBetweenPages
        // and Driver3WorkerCQLIT#testPreparedStatementOldSchemaAfterAlter
        // for more context.
        clusterBuilder = clusterBuilder.withProtocolVersion(ProtocolVersion.V4);

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

        final Metadata metadata = driverCluster.getMetadata();
        for (final Host host : metadata.getAllHosts()) {
            logger.atFine().log("Scylla node - Data center: %s Host: %s Rack: %s", host.getDatacenter(), host.getEndPoint(), host.getRack());
        }

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
