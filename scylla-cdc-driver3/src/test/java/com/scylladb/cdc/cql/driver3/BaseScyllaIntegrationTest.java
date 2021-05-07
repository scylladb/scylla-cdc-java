package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.scylladb.cdc.cql.CQLConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Properties;

public class BaseScyllaIntegrationTest {
    protected Session driverSession;
    protected Cluster driverCluster;
    protected Driver3Session librarySession;

    @BeforeEach
    public void beforeEach() {
        Properties systemProperties = System.getProperties();
        String hostname = systemProperties.getProperty("scylla.docker.hostname");
        int port = Integer.parseInt(systemProperties.getProperty("scylla.docker.port"));

        driverCluster = Cluster.builder()
                .addContactPointsWithPorts(new InetSocketAddress(hostname, port))
                .build();
        driverSession = driverCluster.connect();

        // Drop the test keyspace in case a prior cleanup was not properly executed.
        driverSession.execute(SchemaBuilder.dropKeyspace("ks").ifExists());

        // Create a test keyspace.
        driverSession.execute(SchemaBuilder.createKeyspace("ks").with().replication(
            new HashMap<String, Object>() {{
                put("class", "SimpleStrategy");
                put("replication_factor", "1");
        }}));

        CQLConfiguration cqlConfiguration = CQLConfiguration.builder()
                .addContactPoint(hostname, port)
                .build();

        librarySession = new Driver3Session(cqlConfiguration);
    }

    @AfterEach
    public void afterEach() {
        if (driverSession != null) {
            // Drop the test keyspace.
            driverSession.execute(SchemaBuilder.dropKeyspace("ks").ifExists());
            driverSession.close();
        }
        if (driverCluster != null) {
            driverCluster.close();
        }
        if (librarySession != null) {
            librarySession.close();
        }
    }
}
