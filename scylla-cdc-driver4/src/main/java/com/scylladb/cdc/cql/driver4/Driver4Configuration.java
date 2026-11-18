package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.internal.core.addresstranslation.Ec2MultiRegionAddressTranslator;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.scylladb.cdc.cql.CQLConfiguration;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigResolveOptions;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps a {@link CQLConfiguration} to Java driver 4.x configuration.
 */
final class Driver4Configuration {
    private Driver4Configuration() {
    }

    /**
     * Builds the driver configuration for a library session.
     * <p>
     * Only the driver's reference.conf is used as a fallback. Neither an application.conf nor
     * {@code -Ddatastax-java-driver.*} system properties meant for the application's own sessions
     * change the CDC session.
     */
    static DriverConfigLoader configLoader(CQLConfiguration configuration) {
        Map<String, Object> options = options(configuration);
        return new DefaultDriverConfigLoader(() -> ConfigFactory.parseMap(options)
                .withFallback(ConfigFactory.parseResources(CqlSession.class.getClassLoader(), "reference.conf"))
                .resolve(ConfigResolveOptions.noSystem())
                .getConfig(DefaultDriverConfigLoader.DEFAULT_ROOT_PATH));
    }

    private static Map<String, Object> options(CQLConfiguration configuration) {
        Map<String, Object> options = new HashMap<>();
        // Matches scylla-cdc-driver3's V4 pin.
        put(options, DefaultDriverOption.PROTOCOL_VERSION, DefaultProtocolVersion.V4.name());

        if (configuration.getLocalDCName() != null) {
            put(options, DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, configuration.getLocalDCName());
        }
        if (configuration.getLocalRackName() != null) {
            put(options, DefaultDriverOption.LOAD_BALANCING_LOCAL_RACK, configuration.getLocalRackName());
        }
        if (configuration.getAddressTranslator() == CQLConfiguration.AddressTranslatorType.EC2_MULTI_REGION) {
            put(options, DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS, Ec2MultiRegionAddressTranslator.class.getName());
        }
        if (configuration.queryOptionsFetchSize > 0) {
            put(options, DefaultDriverOption.REQUEST_PAGE_SIZE, configuration.queryOptionsFetchSize);
        }
        // 4.x has one pool size per node instead of a core and a maximum.
        Integer poolSize = configuration.maxPoolLocal != null ? configuration.maxPoolLocal : configuration.corePoolLocal;
        if (poolSize != null) {
            put(options, DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, poolSize);
        }
        if (configuration.poolingMaxRequestsPerConnectionLocal != null) {
            put(options, DefaultDriverOption.CONNECTION_MAX_REQUESTS, configuration.poolingMaxRequestsPerConnectionLocal);
        }
        return options;
    }

    private static void put(Map<String, Object> options, DriverOption option, Object value) {
        options.put(DefaultDriverConfigLoader.DEFAULT_ROOT_PATH + "." + option.getPath(), value);
    }

    /**
     * Lists the settings of {@code configuration} that Java driver 4.x cannot honour.
     */
    static List<String> unsupportedSettings(CQLConfiguration configuration) {
        List<String> unsupported = new ArrayList<>();
        switch (configuration.getReplicaOrdering()) {
            case NEUTRAL:
            case TOPOLOGICAL:
                unsupported.add("Replica ordering " + configuration.getReplicaOrdering()
                        + " has no Java driver 4.x equivalent; the driver's default replica ordering is used.");
                break;
            default:
                break;
        }
        if (configuration.poolingMaxQueueSize != null) {
            unsupported.add("poolingMaxQueueSize is ignored: Java driver 4.x has no per-host request queue.");
        }
        if (configuration.poolTimeoutMillis != null) {
            unsupported.add("poolTimeoutMillis is ignored: Java driver 4.x has no per-host request queue.");
        }
        if (configuration.maxPoolLocal != null && configuration.corePoolLocal != null
                && !configuration.maxPoolLocal.equals(configuration.corePoolLocal)) {
            unsupported.add("corePoolLocal is ignored: Java driver 4.x has a single pool size, taken from maxPoolLocal.");
        }
        for (InetSocketAddress contactPoint : configuration.contactPoints) {
            if (contactPoint.getPort() != configuration.defaultPort) {
                unsupported.add("defaultPort " + configuration.defaultPort + " is ignored: Java driver 4.x gives"
                        + " discovered nodes the port of its first control connection, and contact point "
                        + contactPoint + " uses port " + contactPoint.getPort() + ".");
                break;
            }
        }
        return unsupported;
    }

    static DefaultConsistencyLevel consistencyLevel(CQLConfiguration.ConsistencyLevel consistencyLevel) {
        switch (consistencyLevel) {
            case ONE:
                return DefaultConsistencyLevel.ONE;
            case TWO:
                return DefaultConsistencyLevel.TWO;
            case THREE:
                return DefaultConsistencyLevel.THREE;
            case QUORUM:
                return DefaultConsistencyLevel.QUORUM;
            case ALL:
                return DefaultConsistencyLevel.ALL;
            case LOCAL_QUORUM:
                return DefaultConsistencyLevel.LOCAL_QUORUM;
            case LOCAL_ONE:
                return DefaultConsistencyLevel.LOCAL_ONE;
            default:
                throw new IllegalStateException("Unsupported consistency level: " + consistencyLevel);
        }
    }
}
