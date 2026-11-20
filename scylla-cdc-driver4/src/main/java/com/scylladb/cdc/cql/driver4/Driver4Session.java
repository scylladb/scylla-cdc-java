package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.cql.CQLConfiguration;

/**
 * A Java driver 4.x session built from a {@link CQLConfiguration}, the counterpart of
 * {@code Driver3Session}.
 * <p>
 * Differences from scylla-cdc-driver3:
 * <ul>
 *   <li>Without a local datacenter, the driver uses the datacenter of the first contact point
 *       it reaches; scylla-cdc-driver3 balanced over all datacenters.</li>
 *   <li>{@code defaultPort} is not applied to discovered nodes: the driver gives them the port of
 *       its first control connection, saved once when the session starts. A warning is logged when
 *       a contact point's port differs from {@code defaultPort}.</li>
 *   <li>Settings 4.x cannot honour are ignored, with a warning when the session is built.</li>
 * </ul>
 */
public class Driver4Session implements AutoCloseable {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final CqlSession driverSession;
    private final ConsistencyLevel consistencyLevel;

    public Driver4Session(CQLConfiguration cqlConfiguration) {
        for (String setting : Driver4Configuration.unsupportedSettings(cqlConfiguration)) {
            logger.atWarning().log("%s", setting);
        }
        consistencyLevel = Driver4Configuration.consistencyLevel(cqlConfiguration.getConsistencyLevel());
        driverSession = configure(CqlSession.builder(), cqlConfiguration).build();
    }

    static CqlSessionBuilder configure(CqlSessionBuilder builder, CQLConfiguration cqlConfiguration) {
        builder = builder
                .withConfigLoader(Driver4Configuration.configLoader(cqlConfiguration))
                .addContactPoints(cqlConfiguration.contactPoints);
        if (cqlConfiguration.user != null && cqlConfiguration.password != null) {
            builder = builder.withAuthCredentials(cqlConfiguration.user, cqlConfiguration.password);
        }
        if (cqlConfiguration.sslConfig != null) {
            builder = builder.withSslEngineFactory(new Driver4SslEngineFactory(cqlConfiguration.sslConfig));
        }
        return builder;
    }

    protected CqlSession getDriverSession() {
        return driverSession;
    }

    /**
     * @return the consistency level for reads of the CDC log table
     */
    protected ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    @Override
    public void close() {
        driverSession.close();
    }
}
