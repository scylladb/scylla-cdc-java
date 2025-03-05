package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.cql.SslConfig;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

public class Driver3Session implements AutoCloseable {
    private final Cluster driverCluster;
    private final Session driverSession;
    private final ConsistencyLevel consistencyLevel;

    public Driver3Session(CQLConfiguration cqlConfiguration) {
        Cluster.Builder clusterBuilder = Cluster.builder()
                .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED);
        clusterBuilder = clusterBuilder.addContactPointsWithPorts(cqlConfiguration.contactPoints);

        if (cqlConfiguration.sslConfig != null) {
            SslConfig sslConfig = cqlConfiguration.sslConfig;
            final SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();
            System.out.println(sslConfig.getClass().getProtectionDomain().getCodeSource().getLocation());
            sslContextBuilder.sslProvider(SslProvider.valueOf(sslConfig.sslProviderString));
            if (sslConfig.trustStorePath != null) {
                final KeyStore trustKeyStore = createKeyStore(sslConfig.trustStorePath, sslConfig.trustStorePassword);

                final TrustManagerFactory trustManagerFactory;
                try {
                    trustManagerFactory =
                            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    trustManagerFactory.init(trustKeyStore);
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException("Exception while creating TrustManagerFactory", e);
                } catch (KeyStoreException e) {
                    throw new RuntimeException("Exception while calling TrustManagerFactory.init()", e);
                }
                sslContextBuilder.trustManager(trustManagerFactory);
            }

            if (sslConfig.keyStorePath != null) {
                final KeyStore keyStore = createKeyStore(sslConfig.keyStorePath, sslConfig.keyStorePassword);

                final KeyManagerFactory keyManagerFactory;
                try {
                    keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    keyManagerFactory.init(keyStore, sslConfig.keyStorePassword.toCharArray());
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException("Exception while creating KeyManagerFactory", e);
                } catch (UnrecoverableKeyException | KeyStoreException e) {
                    throw new RuntimeException("Exception while calling KeyManagerFactory.init()", e);
                }
                sslContextBuilder.keyManager(keyManagerFactory);
            }

            if (sslConfig.cipherSuites.size() > 0) {
                sslContextBuilder.ciphers(sslConfig.cipherSuites);
            }

            if (sslConfig.certPath != null && sslConfig.privateKeyPath != null) {
                try {
                    sslContextBuilder.keyManager(new BufferedInputStream(new FileInputStream(sslConfig.certPath)),
                            new BufferedInputStream(new FileInputStream(sslConfig.privateKeyPath)));
                } catch (IllegalArgumentException e) {
                    throw new RuntimeException(String.format("Invalid certificate or private key: %s", e.getMessage()));
                } catch (FileNotFoundException e) {
                    throw new RuntimeException("Invalid certificate or private key file path", e);
                }
            } else if ((sslConfig.certPath == null) != (sslConfig.privateKeyPath == null)) {
                throw new RuntimeException(String.format("%s cannot be set without %s and vice-versa: %s is not set",
                        "scylla.ssl.openssl.keyCertChain", "scylla.ssl.openssl.privateKey",
                        (sslConfig.certPath == null) ? "scylla.ssl.openssl.keyCertChain" : "scylla.ssl.openssl.privateKey"));
            }

            final SslContext context;
            try {
                context = sslContextBuilder.build();
            } catch (SSLException e) {
                throw new RuntimeException(e);
            }
            final SSLOptions sslOptions = new RemoteEndpointAwareNettySSLOptions(context);
            clusterBuilder.withSSL(sslOptions);
        }

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
                new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().withLocalDc(cqlConfiguration.getLocalDCName()).build())
            );
        }

        if (cqlConfiguration.queryOptionsFetchSize > 0) {
            QueryOptions queryOptions = new QueryOptions();
            queryOptions.setFetchSize(cqlConfiguration.queryOptionsFetchSize);
            clusterBuilder = clusterBuilder.withQueryOptions(queryOptions);
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

    private KeyStore createKeyStore(String path, String password) {
        KeyStore keyStore;
        try {
            keyStore = KeyStore.getInstance("JKS");
            try (InputStream inputStream = Files.newInputStream(Paths.get(path))) {
                keyStore.load(inputStream, password.toCharArray());
            } catch (IOException e) {
                throw new RuntimeException("Exception while reading keystore", e);
            } catch (CertificateException | NoSuchAlgorithmException e) {
                throw new RuntimeException("Exception while loading keystore", e);
            }
        } catch (KeyStoreException e) {
            throw new RuntimeException("Exception while creating keystore", e);
        }
        return keyStore;
    }
}
