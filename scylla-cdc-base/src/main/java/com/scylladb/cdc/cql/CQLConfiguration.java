package com.scylladb.cdc.cql;

import com.google.common.base.Preconditions;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CQLConfiguration {
    private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

    /**
     * The consistency level of read queries to Scylla.
     */
    public enum ConsistencyLevel {
        /**
         * Waits for a response from single replica
         * in the local data center.
         */
        LOCAL_ONE,
        /**
         * Waits for a response from single replica.
         */
        ONE,
        /**
         * Waits for responses from two replicas.
         */
        TWO,
        /**
         * Waits for responses from three replicas.
         */
        THREE,
        /**
         * Waits for responses from a quorum of replicas
         * in the same DC as the coordinator.
         * <p>
         * Local quorum is defined as:
         * <code>dataCenterReplicationFactor / 2 + 1</code>,
         * where <code>dataCenterReplicationFactor</code> is the
         * configured replication factor for the datacenter of
         * the coordinator node.
         */
        LOCAL_QUORUM,
        /**
         * Waits for responses from a quorum of replicas.
         * <p>
         * Quorum is defined as:
         * <code>(dc1ReplicationFactor + dc2ReplicationFactor + ...) / 2 + 1</code>,
         * where <code>dc1ReplicationFactor</code>, <code>dc2ReplicationFactor</code>, ... are the configured
         * replication factors for all data centers.
         */
        QUORUM,
        /**
         * Waits for responses from all replicas.
         */
        ALL
    }

    public final List<InetSocketAddress> contactPoints;
    public final String user;
    public final String password;
    private final ConsistencyLevel consistencyLevel;
    private final String localDCName;
    private final String localRackName;
    private final ReplicaOrdering replicaOrdering;
    public final SslConfig sslConfig;
    public final int queryOptionsFetchSize;
    // Part of driver's pooling options
    public final Integer corePoolLocal;
    public final Integer maxPoolLocal;
    public final Integer poolingMaxQueueSize;
    public final Integer poolingMaxRequestsPerConnectionLocal;
    public final Integer poolTimeoutMillis;
    public final int defaultPort;

    private CQLConfiguration(List<InetSocketAddress> contactPoints,
                             String user, String password, ConsistencyLevel consistencyLevel,
                             String localDCName, String localRackName, ReplicaOrdering replicaOrdering,
                             SslConfig sslConfig, int queryOptionsFetchSize, Integer corePoolLocal,
                             Integer maxPoolLocal, Integer poolingMaxQueueSize,
                             Integer poolingMaxRequestsPerConnectionLocal,
                             Integer poolTimeoutMillis,
                             int defaultPort
                             ) {
        this.contactPoints = Preconditions.checkNotNull(contactPoints);
        Preconditions.checkArgument(!contactPoints.isEmpty());
        this.defaultPort = defaultPort;

        this.user = user;
        this.password = password;
        // Either someone did not provide credentials
        // or provided user-password pair.
        Preconditions.checkArgument((this.user == null && this.password == null)
                || (this.user != null && this.password != null));

        this.consistencyLevel = Preconditions.checkNotNull(consistencyLevel);
        this.localDCName = localDCName;
        this.localRackName = localRackName;
        this.replicaOrdering = Preconditions.checkNotNull(replicaOrdering);
        this.sslConfig = sslConfig;
        this.queryOptionsFetchSize = queryOptionsFetchSize;
        this.corePoolLocal = corePoolLocal;
        this.maxPoolLocal = maxPoolLocal;
        this.poolingMaxQueueSize = poolingMaxQueueSize;
        this.poolingMaxRequestsPerConnectionLocal = poolingMaxRequestsPerConnectionLocal;
        this.poolTimeoutMillis = poolTimeoutMillis;
    }

    /**
     * Returns the configured consistency level.
     * <p>
     * This consistency level is used in read queries to the
     * CDC log table. The queries to system tables, such
     * as <code>system_distributed.cdc_streams_descriptions_v2</code> do
     * not respect this configuration option.
     *
     * @return configured consistency level.
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    /**
     * Returns the name of the configured local datacenter.
     * <p>
     * This local datacenter name will be used to setup
     * the connection to Scylla to prioritize sending requests to
     * the nodes in the local datacenter. If this parameter
     * was not configured, this method returns <code>null</code>.
     *
     * @return the name of configured local datacenter or
     * <code>null</code> if it was not configured.
     */
    public String getLocalDCName() {
        return localDCName;
    }

    /**
     * Returns default port.
     * <p>
     * This port is used with every host driver auto discovers that host no port information available
     *
     * @return default port
     * <code>null</code> if it was not configured.
     */
    public int getDefaultPort() {
        return defaultPort;
    }

    /**
     * Returns the name of the configured local rack.
     * <p>
     * This local rack name will be used to setup
     * the connection to Scylla to prioritize sending requests to
     * the nodes in the local rack (in the local datacenter). If this parameter
     * was not configured, this method returns <code>null</code>.
     *
     * @return the name of configured local rack or
     * <code>null</code> if it was not configured.
     */
    public String getLocalRackName() {
        return localRackName;
    }

    /**
     * Returns replica ordering.
     * <p>
     * Replica ordering defines how CQL driver iterates over data replicas
     * when reads from CDC tables.
     *
     * @return replica ordering
     */
    public ReplicaOrdering getReplicaOrdering() {
        return replicaOrdering;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<InetSocketAddress> contactPoints = new ArrayList<>();
        private int defaultPort = 9042;
        private String user = null;
        private String password = null;
        private ConsistencyLevel consistencyLevel = DEFAULT_CONSISTENCY_LEVEL;
        private String localDCName = null;
        private String localRackName = null;
        private ReplicaOrdering replicaOrdering = ReplicaOrdering.RANDOM;
        private SslConfig sslConfig = null;
        private int queryOptionsFetchSize = 0;
        private Integer corePoolLocal = null;
        private Integer maxPoolLocal = null;
        private Integer poolingMaxQueueSize = null;
        private Integer poolingMaxRequestsPerConnectionLocal = null;
        private Integer poolTimeoutMillis = null;

        public Builder addContactPoint(InetSocketAddress contactPoint) {
            Preconditions.checkNotNull(contactPoint);
            contactPoints.add(contactPoint);
            return this;
        }

        public Builder addContactPoints(Collection<InetSocketAddress> addedContactPoints) {
            for (InetSocketAddress contactPoint : addedContactPoints) {
                this.addContactPoint(contactPoint);
            }
            return this;
        }

        public Builder addContactPoint(String host, int port) {
            Preconditions.checkNotNull(host);
            Preconditions.checkArgument(port > 0 && port < 65536);
            return addContactPoint(new InetSocketAddress(host, port));
        }

        public Builder addContactPoint(String host) {
            return addContactPoint(host, defaultPort);
        }

        /**
         * Changes default port not only for contact points, but also for nodes driver will discover.
         * To be set before calling `addContactPoint`
         *
         * @param port a port number that will be a default
         * @return self
         */
        public Builder withDefaultPort(int port) {
            this.defaultPort = port;
            return this;
        }

        public Builder withCredentials(String user, String password) {
            this.user = Preconditions.checkNotNull(user);
            this.password = Preconditions.checkNotNull(password);
            return this;
        }

        /**
         * Sets the consistency level of CDC table read queries.
         * <p>
         * This consistency level is used only for read queries
         * to the CDC log table. The queries to system tables, such
         * as <code>system_distributed.cdc_streams_descriptions_v2</code> do
         * not respect this configuration option.
         *
         * @param consistencyLevel consistency level to set.
         * @return a reference to this builder.
         */
        public Builder withConsistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = Preconditions.checkNotNull(consistencyLevel);
            return this;
        }

        /**
         * Sets the name of local datacenter.
         * <p>
         * This local datacenter name will be used to setup
         * the connection to Scylla to prioritize sending requests to
         * the nodes in the local datacenter.
         *
         * @param localDCName the name of local datacenter to set.
         * @return a reference to this builder.
         */
        public Builder withLocalDCName(String localDCName) {
            this.localDCName = Preconditions.checkNotNull(localDCName);
            return this;
        }

        /**
         * Sets the name of local rack.
         * <p>
         * This local rack name will be used to setup
         * the connection to Scylla to prioritize sending requests to
         * the nodes in the local rack (in local datacenter).
         *
         * @param localRackName the name of local rack to set.
         * @return a reference to this builder.
         */
        public Builder withLocalRackName(String localRackName) {
            this.localRackName = Preconditions.checkNotNull(localRackName);
            return this;
        }

        /**
         * Sets the replica ordering for load balancing policy.
         * <p>
         * It allows to change the way reader iterates over data replicas
         * when read data from
         *
         * @param replicaOrdering replica ordering to set.
         * @return a reference to this builder.
         */
        public Builder withReplicaOrdering(ReplicaOrdering replicaOrdering) {
            this.replicaOrdering = Preconditions.checkNotNull(replicaOrdering);
            return this;
        }

        public Builder withSslConfig(SslConfig sslConfig) {
            this.sslConfig = sslConfig;
            return this;
        }

        /**
         * Sets the default page fetch size for select queries. Will be passed to {@code QueryOptions}
         * of the driver's {@code Cluster.Builder}. 0 means default to driver's default.
         * @param fetchSize select query page fetch size.
         * @return a reference to this builder.
         */
        public Builder withQueryOptionsFetchSize(int fetchSize) {
            this.queryOptionsFetchSize = fetchSize;
            return this;
        }

        /**
         * Target number of connections per pool passed to driver's PoolingOptions.
         * {@code null} means use driver's defaults.
         * @param corePoolLocal
         * @return a reference to this builder.
         */
        public Builder withCorePoolLocal(Integer corePoolLocal) {
            this.corePoolLocal = corePoolLocal;
            return this;
        }

        /**
         * Max number of connections per pool passed to driver's PoolingOptions.
         * {@code null} means use driver's defaults.
         * @param maxPoolLocal
         * @return a reference to this builder.
         */
        public Builder withMaxPoolLocal(Integer maxPoolLocal) {
            this.maxPoolLocal = maxPoolLocal;
            return this;
        }

        /**
         * Max queue size passed to driver's PoolingOptions. Driver will queue up to this many
         * requests at once until it starts returning BusyPoolException.
         * {@code null} means use driver's defaults.
         * @param poolingMaxQueueSize
         * @return a reference to this builder.
         */
        public Builder withPoolingMaxQueueSize(Integer poolingMaxQueueSize) {
            this.poolingMaxQueueSize = poolingMaxQueueSize;
            return this;
        }

        /**
         * Max number of requests per connection passed to driver's PoolingOptions.
         * {@code null} means use driver's defaults.
         * @param poolingMaxRequestsPerConnectionLocal
         * @return a reference to this builder.
         */
        public Builder withPoolingMaxRequestsPerConnectionLocal(Integer poolingMaxRequestsPerConnectionLocal) {
            this.poolingMaxRequestsPerConnectionLocal = poolingMaxRequestsPerConnectionLocal;
            return this;
        }

        /**
         * Pool timeout in milliseconds passed to driver's PoolingOptions.
         * Requests waiting longer for available connection than this value will be rejected.
         * {@code null} means use driver's defaults.
         * @param poolTimeoutMillis
         * @return a reference to this builder.
         */
        public Builder withPoolTimeoutMillis(Integer poolTimeoutMillis) {
            this.poolTimeoutMillis = poolTimeoutMillis;
            return this;
        }

        public CQLConfiguration build() {
            return new CQLConfiguration(contactPoints, user, password, consistencyLevel, localDCName, localRackName, replicaOrdering, sslConfig, queryOptionsFetchSize, corePoolLocal, maxPoolLocal, poolingMaxQueueSize, poolingMaxRequestsPerConnectionLocal, poolTimeoutMillis, defaultPort);
        }
    }
}