package com.scylladb.cdc.replicator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.cql.driver3.Driver3Session;
import com.scylladb.cdc.lib.CDCConsumer;
import com.scylladb.cdc.model.TableName;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import sun.misc.Signal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class Main {
    public static void main(String[] args) {
        // Parse command-line arguments.
        Namespace parsedArguments = parseArguments(args);
        Mode replicatorMode = Mode.fromString(parsedArguments.getString("mode"));
        String source = parsedArguments.getString("source");
        String destination = parsedArguments.getString("destination");
        String keyspace = parsedArguments.getString("keyspace");
        String table = parsedArguments.getString("table");
        ConsistencyLevel consistencyLevel = ConsistencyLevel.valueOf(parsedArguments.getString("consistency_level").toUpperCase());

        // Start replicating changes from source cluster to destination cluster
        // of selected tables.
        startReplicator(replicatorMode, source, destination, keyspace, table, consistencyLevel);
    }

    private static void startReplicator(Mode mode, String source, String destination, String keyspace, String tables,
                                        ConsistencyLevel consistencyLevel) {
        // Connect to the destination cluster.
        try (Cluster destinationCluster = Cluster.builder().addContactPoint(destination).build();
             Session destinationSession = destinationCluster.connect()) {

            List<CDCConsumer> startedConsumers = new ArrayList<>();
            String[] tablesToReplicate = tables.split(",");

            for (String table : tablesToReplicate) {
                validateTableExists(destinationCluster, keyspace, table,
                        "Before running the replicator, create the corresponding tables in your destination cluster.");

                // Start a CDCConsumer for each replicated table,
                // which will read the RawChanges and apply them
                // onto the destination cluster.
                CDCConsumer consumer = CDCConsumer.builder()
                        .addContactPoint(source)
                        .withConsumerProvider((threadId) ->
                                new ReplicatorConsumer(mode, destinationCluster, destinationSession,
                                        keyspace, table, consistencyLevel))
                        .addTable(new TableName(keyspace, table))
                        .withWorkersCount(1)
                        .build();

                Optional<Throwable> validation = consumer.validate();
                if (validation.isPresent()) {
                    throw new ReplicatorValidationException("Validation error of the source table: " + validation.get().getMessage());
                }
                consumer.start();

                startedConsumers.add(consumer);
            }

            // Wait for SIGINT and gracefully terminate CDCConsumers.
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
        } catch (ReplicatorValidationException ex) {
            System.err.println(ex.getMessage());
            System.exit(1);
        }
    }

    private static void validateTableExists(Cluster cluster, String keyspace, String table, String hint) throws ReplicatorValidationException {
        String clusterName = cluster.getClusterName() + " (" + cluster.getMetadata().getAllHosts().toString() + ")";

        KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspace);
        if (keyspaceMetadata == null) {
            throw new ReplicatorValidationException(String.format("Missing keyspace %s in cluster: %s. %s",
                    keyspace, clusterName, hint));
        }

        TableMetadata tableMetadata = keyspaceMetadata.getTable(table);
        if (tableMetadata == null) {
            throw new ReplicatorValidationException(String.format("Missing table %s.%s in cluster: %s. %s",
                    keyspace, table, clusterName, hint));
        }
    }

    private static Namespace parseArguments(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("./scylla-cdc-replicator").build().defaultHelp(true);
        parser.addArgument("-m", "--mode").setDefault("delta").help("Mode of operation. Can be delta, preimage or postimage. Default is delta");
        parser.addArgument("-k", "--keyspace").required(true).help("Keyspace name");
        parser.addArgument("-t", "--table").required(true).help("Table names, provided as a comma delimited string");
        parser.addArgument("-s", "--source").required(true).help("Address of a node in source cluster");
        parser.addArgument("-d", "--destination").required(true).help("Address of a node in destination cluster");
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

    public enum Mode {
        DELTA, PRE_IMAGE, POST_IMAGE;

        public static Mode fromString(String mode) {
            switch (mode.toLowerCase()) {
                case "delta":
                    return DELTA;

                case "pre_image":
                case "preimage":
                    return PRE_IMAGE;

                case "post_image":
                case "postimage":
                    return POST_IMAGE;

                default:
                    throw new IllegalStateException("Unknown mode: " + mode);
            }
        }
    }
}