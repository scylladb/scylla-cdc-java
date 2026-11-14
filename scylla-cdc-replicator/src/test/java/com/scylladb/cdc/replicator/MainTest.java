package com.scylladb.cdc.replicator;

import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.lib.CDCConsumer;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class MainTest {
    @Test
    public void sourceDefaultsRemainIndependentOfDestinationWrites() throws Exception {
        Namespace arguments = parseArguments();
        CQLConfiguration source = sourceConfiguration(arguments);

        assertNull(arguments.getString("source_read_consistency"));
        assertNull(arguments.getString("source_datacenter"));
        assertEquals(CQLConfiguration.ConsistencyLevel.LOCAL_QUORUM, source.getConsistencyLevel());
        assertNull(source.getLocalDCName());
        assertEquals("local_quorum", arguments.getString("consistency_level"));
    }

    @ParameterizedTest
    @EnumSource(CQLConfiguration.ConsistencyLevel.class)
    public void explicitReadConsistencyReachesSourceConsumer(CQLConfiguration.ConsistencyLevel consistency) throws Exception {
        Namespace arguments = parseArguments("--source-read-consistency", consistency.name().toLowerCase(Locale.ROOT),
                "--source-datacenter", "source_dc", "-cl", "all");
        CQLConfiguration source = sourceConfiguration(arguments);

        assertEquals(consistency, source.getConsistencyLevel());
        assertEquals("source_dc", source.getLocalDCName());
        assertEquals("all", arguments.getString("consistency_level"));
    }

    @Test
    public void sourceDatacenterPreservesDefaultReadConsistency() throws Exception {
        CQLConfiguration source = sourceConfiguration(parseArguments("--source-datacenter", "source_dc"));

        assertEquals("source_dc", source.getLocalDCName());
        assertEquals(CQLConfiguration.ConsistencyLevel.LOCAL_QUORUM, source.getConsistencyLevel());
    }

    @Test
    public void destinationConsistencyDoesNotOverrideSourceReads() throws Exception {
        Namespace arguments = parseArguments("-cl", "quorum");
        CQLConfiguration source = sourceConfiguration(arguments);

        assertEquals("quorum", arguments.getString("consistency_level"));
        assertEquals(CQLConfiguration.ConsistencyLevel.LOCAL_QUORUM, source.getConsistencyLevel());
        assertNull(source.getLocalDCName());
    }

    private static Namespace parseArguments(String... options) {
        List<String> arguments = new ArrayList<>(Arrays.asList(
                "-k", "ks", "-t", "table", "-s", "127.0.0.1", "-d", "127.0.0.2"));
        arguments.addAll(Arrays.asList(options));
        return Main.parseArguments(arguments.toArray(new String[0]));
    }

    private static CQLConfiguration sourceConfiguration(Namespace arguments) throws Exception {
        CDCConsumer.Builder builder = Main.sourceConsumerBuilder(arguments);
        // Inspect the real source builder without opening a database session.
        Field field = CDCConsumer.Builder.class.getDeclaredField("cqlConfigurationBuilder");
        field.setAccessible(true);
        return ((CQLConfiguration.Builder) field.get(builder)).build();
    }
}
