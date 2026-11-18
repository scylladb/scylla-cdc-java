package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.addresstranslation.Ec2MultiRegionAddressTranslator;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.cql.ReplicaOrdering;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Driver4ConfigurationTest {

    private static CQLConfiguration.Builder builder() {
        return CQLConfiguration.builder().addContactPoint("127.0.0.1");
    }

    private static DriverExecutionProfile referenceProfile() {
        try (DriverConfigLoader loader = new DefaultDriverConfigLoader(
                () -> ConfigFactory.defaultReference(CqlSession.class.getClassLoader())
                        .getConfig(DefaultDriverConfigLoader.DEFAULT_ROOT_PATH))) {
            return loader.getInitialConfig().getDefaultProfile();
        }
    }

    private static DriverExecutionProfile profile(CQLConfiguration configuration) {
        try (DriverConfigLoader loader = Driver4Configuration.configLoader(configuration)) {
            return loader.getInitialConfig().getDefaultProfile();
        }
    }

    private static Map<String, Object> entries(DriverExecutionProfile profile) {
        Map<String, Object> entries = new TreeMap<>();
        profile.entrySet().forEach(entry -> entries.put(entry.getKey(), entry.getValue()));
        return entries;
    }

    @Test
    public void testDefaultsPinProtocolV4AndLeaveOtherOptionsToTheDriver() {
        DriverExecutionProfile profile = profile(builder().build());

        assertEquals("V4", profile.getString(DefaultDriverOption.PROTOCOL_VERSION));
        assertFalse(profile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER));
        assertFalse(profile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_RACK));
        assertEquals("PassThroughAddressTranslator", profile.getString(DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS));
        Map<String, Object> expected = entries(referenceProfile());
        Map<String, Object> actual = entries(profile);
        expected.remove(DefaultDriverOption.PROTOCOL_VERSION.getPath());
        actual.remove(DefaultDriverOption.PROTOCOL_VERSION.getPath());
        assertEquals(expected, actual);
    }

    @Test
    public void testSystemPropertiesDoNotReachTheLibrarySession() {
        String protocolVersion = "datastax-java-driver." + DefaultDriverOption.PROTOCOL_VERSION.getPath();
        String requestTimeout = "datastax-java-driver." + DefaultDriverOption.REQUEST_TIMEOUT.getPath();
        Duration referenceTimeout = referenceProfile().getDuration(DefaultDriverOption.REQUEST_TIMEOUT);
        System.setProperty(protocolVersion, "V5");
        System.setProperty(requestTimeout, "7 seconds");
        ConfigFactory.invalidateCaches();
        try {
            // The properties reach sessions built with the driver's default loader.
            try (DriverConfigLoader loader = DriverConfigLoader.fromDefaults(getClass().getClassLoader())) {
                assertEquals(Duration.ofSeconds(7),
                        loader.getInitialConfig().getDefaultProfile().getDuration(DefaultDriverOption.REQUEST_TIMEOUT));
            }

            DriverExecutionProfile profile = profile(builder().build());

            assertEquals("V4", profile.getString(DefaultDriverOption.PROTOCOL_VERSION));
            assertEquals(referenceTimeout, profile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT));
        } finally {
            System.clearProperty(protocolVersion);
            System.clearProperty(requestTimeout);
            ConfigFactory.invalidateCaches();
        }
    }

    @Test
    public void testApplicationConfDoesNotReachTheLibrarySession() {
        // src/test/resources/application.conf sets basic.session-name.
        DriverExecutionProfile profile = profile(builder().build());

        assertFalse(profile.isDefined(DefaultDriverOption.SESSION_NAME));
        try (DriverConfigLoader loader = DriverConfigLoader.fromDefaults(getClass().getClassLoader())) {
            assertEquals("from-application-conf",
                    loader.getInitialConfig().getDefaultProfile().getString(DefaultDriverOption.SESSION_NAME));
        }
    }

    @Test
    public void testMapsEverySupportedSetting() {
        CQLConfiguration configuration = builder()
                .withLocalDCName("dc1")
                .withLocalRackName("rack1")
                .withAddressTranslator(CQLConfiguration.AddressTranslatorType.EC2_MULTI_REGION)
                .withQueryOptionsFetchSize(123)
                .withCorePoolLocal(2)
                .withMaxPoolLocal(3)
                .withPoolingMaxRequestsPerConnectionLocal(456)
                .build();

        DriverExecutionProfile profile = profile(configuration);

        assertEquals("dc1", profile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER));
        assertEquals("rack1", profile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_RACK));
        assertEquals(Ec2MultiRegionAddressTranslator.class.getName(),
                profile.getString(DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS));
        assertEquals(123, profile.getInt(DefaultDriverOption.REQUEST_PAGE_SIZE));
        assertEquals(3, profile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE));
        assertEquals(456, profile.getInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS));
    }

    @Test
    public void testCorePoolSizeIsUsedWithoutMaxPoolSize() {
        DriverExecutionProfile profile = profile(builder().withCorePoolLocal(4).build());

        assertEquals(4, profile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE));
    }

    @Test
    public void testSupportedSettingsProduceNoWarnings() {
        CQLConfiguration configuration = builder()
                .withLocalDCName("dc1")
                .withReplicaOrdering(ReplicaOrdering.RANDOM)
                .withCorePoolLocal(2)
                .withMaxPoolLocal(2)
                .build();

        assertTrue(Driver4Configuration.unsupportedSettings(configuration).isEmpty());
    }

    @Test
    public void testUnsupportedSettingsAreReported() {
        CQLConfiguration configuration = builder()
                .withReplicaOrdering(ReplicaOrdering.TOPOLOGICAL)
                .withPoolingMaxQueueSize(10)
                .withPoolTimeoutMillis(20)
                .withCorePoolLocal(1)
                .withMaxPoolLocal(2)
                .build();

        List<String> unsupported = Driver4Configuration.unsupportedSettings(configuration);

        assertEquals(4, unsupported.size(), unsupported.toString());
        assertTrue(unsupported.get(0).contains("TOPOLOGICAL"));
        assertTrue(unsupported.get(1).startsWith("poolingMaxQueueSize"));
        assertTrue(unsupported.get(2).startsWith("poolTimeoutMillis"));
        assertTrue(unsupported.get(3).startsWith("corePoolLocal"));
    }

    @Test
    public void testContactPointOnAnotherPortThanDefaultPortIsReported() {
        CQLConfiguration configuration = builder()
                .addContactPoint("127.0.0.2", 9142)
                .build();

        List<String> unsupported = Driver4Configuration.unsupportedSettings(configuration);

        assertEquals(1, unsupported.size(), unsupported.toString());
        assertTrue(unsupported.get(0).startsWith("defaultPort 9042 is ignored"), unsupported.get(0));
    }

    @Test
    public void testContactPointsOnDefaultPortProduceNoWarnings() {
        CQLConfiguration configuration = CQLConfiguration.builder()
                .withDefaultPort(9142)
                .addContactPoint("127.0.0.1")
                .addContactPoint("127.0.0.2", 9142)
                .build();

        assertTrue(Driver4Configuration.unsupportedSettings(configuration).isEmpty());
    }

    /**
     * Fails when a {@link CQLConfiguration} field is added without deciding whether
     * {@link Driver4Session} maps it or warns that it is ignored.
     */
    @Test
    public void testEveryConfigurationFieldIsMappedOrWarned() {
        Set<String> mapped = new HashSet<>(Arrays.asList("contactPoints", "user", "password", "consistencyLevel",
                "localDCName", "localRackName", "addressTranslator", "sslConfig", "queryOptionsFetchSize",
                "corePoolLocal", "maxPoolLocal", "poolingMaxRequestsPerConnectionLocal"));
        Set<String> warned = new HashSet<>(Arrays.asList("replicaOrdering", "poolingMaxQueueSize",
                "poolTimeoutMillis", "defaultPort"));
        Set<String> handled = new HashSet<>(mapped);
        handled.addAll(warned);

        Set<String> fields = Arrays.stream(CQLConfiguration.class.getDeclaredFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers()) && !field.isSynthetic())
                .map(Field::getName)
                .collect(Collectors.toSet());

        assertEquals(handled, fields);
    }

    @Test
    public void testNeutralReplicaOrderingIsReported() {
        CQLConfiguration configuration = builder()
                .withReplicaOrdering(ReplicaOrdering.NEUTRAL)
                .build();

        List<String> unsupported = Driver4Configuration.unsupportedSettings(configuration);

        assertEquals(1, unsupported.size(), unsupported.toString());
        assertTrue(unsupported.get(0).contains("NEUTRAL"));
    }

    @Test
    public void testEveryConsistencyLevelMaps() {
        for (CQLConfiguration.ConsistencyLevel level : CQLConfiguration.ConsistencyLevel.values()) {
            assertEquals(level.name(), Driver4Configuration.consistencyLevel(level).name());
        }
    }
}
