package com.scylladb.cdc.replicator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.scylladb.cdc.lib.CDCConsumer;
import com.scylladb.cdc.lib.CDCConsumerBuilder;
import com.scylladb.cdc.model.TableName;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import sun.misc.Signal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class Main {
    public enum Mode {
        DELTA, PREIMAGE, POSTIMAGE;

        public static Mode fromString(String mode) {
            if ("delta".equals(mode)) {
                return DELTA;
            } else if ("preimage".equals(mode)) {
                return PREIMAGE;
            } else if ("postimage".equals(mode)) {
                return POSTIMAGE;
            } else {
                throw new IllegalStateException("Wrong mode " + mode);
            }
        }
    }

    private static void replicateChanges(Mode mode, String source, String destination, String keyspace, String tables,
                                         ConsistencyLevel cl) {
        try (Cluster sCluster = Cluster.builder().addContactPoint(source).build();
             Session sSession = sCluster.connect();
             Cluster dCluster = Cluster.builder().addContactPoint(destination).build();
             Session dSession = dCluster.connect()) {

            List<CDCConsumer> startedConsumers = new ArrayList<>();
            String[] parsedTables = tables.split(",");

            // TODO - currently building a separate CDCConsumer per each table.
            // Should be able to construct a single CDCConsumer and ReplicatorConsumer
            // to "multiplex" - maybe there should be a helper class that does the multiplexing?
            // and the ReplicatorConsumer doesn't have to do it manually.
            //
            // ConsumerMultiplexer((tableName) -> new ReplicatorConsumer(...))
            for (String table : parsedTables) {
                Set<TableName> cdcTableSet = Collections.singleton(new TableName(keyspace, table));

                CDCConsumer consumer = CDCConsumerBuilder.builder(sSession, (threadId) -> new ReplicatorConsumer(mode, dCluster, dSession, keyspace, table, cl), cdcTableSet).workersCount(1).build();
                consumer.start();

                startedConsumers.add(consumer);
            }

            try {
                CountDownLatch terminationLatch = new CountDownLatch(1);
                Signal.handle(new Signal("INT"), signal -> terminationLatch.countDown());
                terminationLatch.await();

                for (CDCConsumer consumer : startedConsumers) {
                    consumer.stop();
                }
            } catch (InterruptedException e) {
                // Ignore exception.
            }
        }
    }

    private static Namespace parseArguments(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("CDCReplicator").build().defaultHelp(true);
        parser.addArgument("-m", "--mode").setDefault("delta").help("Mode of operation. Can be delta, preimage or postimage. Default is delta");
        parser.addArgument("-k", "--keyspace").help("Keyspace name");
        parser.addArgument("-t", "--table").help("Table names, provided as a comma delimited string");
        parser.addArgument("-s", "--source").help("Address of a node in source cluster");
        parser.addArgument("-d", "--destination").help("Address of a node in destination cluster");
        parser.addArgument("-cl", "--consistency-level").setDefault("quorum")
                .help("Consistency level of writes. QUORUM by default");

        try {
            return parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(-1);
            return null;
        }
    }

    public static void main(String[] args) {
        Namespace ns = parseArguments(args);
        replicateChanges(Mode.fromString(ns.getString("mode")), ns.getString("source"),
                ns.getString("destination"), ns.getString("keyspace"), ns.getString("table"),
                ConsistencyLevel.valueOf(ns.getString("consistency_level").toUpperCase()));
    }

}