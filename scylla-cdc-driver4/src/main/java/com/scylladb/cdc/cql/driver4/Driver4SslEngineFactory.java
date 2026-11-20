package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.scylladb.cdc.cql.SslConfig;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.ReferenceCountUtil;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

/**
 * Builds SSL engines from an {@link SslConfig}, as scylla-cdc-driver3 does.
 * <p>
 * Like scylla-cdc-driver3, it does not validate host names. The driver's
 * {@code DefaultSslEngineFactory} validates them by default, so it is not used here, and the
 * check is turned off explicitly because Netty 4.2 turns it on by default for clients.
 */
class Driver4SslEngineFactory implements SslEngineFactory {
    private final SslContext context;

    Driver4SslEngineFactory(SslConfig sslConfig) {
        SslContextBuilder builder = SslContextBuilder.forClient().endpointIdentificationAlgorithm(null);
        if (sslConfig.sslProviderString != null) {
            builder.sslProvider(SslProvider.valueOf(sslConfig.sslProviderString));
        }
        if (sslConfig.trustStorePath != null) {
            KeyStore trustStore = createKeyStore(sslConfig.trustStorePath, sslConfig.trustStorePassword);
            try {
                TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                factory.init(trustStore);
                builder.trustManager(factory);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Exception while creating TrustManagerFactory", e);
            } catch (KeyStoreException e) {
                throw new RuntimeException("Exception while calling TrustManagerFactory.init()", e);
            }
        }
        if (sslConfig.keyStorePath != null) {
            KeyStore keyStore = createKeyStore(sslConfig.keyStorePath, sslConfig.keyStorePassword);
            try {
                KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                factory.init(keyStore, toCharArray(sslConfig.keyStorePassword));
                builder.keyManager(factory);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Exception while creating KeyManagerFactory", e);
            } catch (UnrecoverableKeyException | KeyStoreException e) {
                throw new RuntimeException("Exception while calling KeyManagerFactory.init()", e);
            }
        }
        if (sslConfig.cipherSuites != null && !sslConfig.cipherSuites.isEmpty()) {
            builder.ciphers(sslConfig.cipherSuites);
        }
        if (sslConfig.certPath != null && sslConfig.privateKeyPath != null) {
            try (InputStream cert = new BufferedInputStream(new FileInputStream(sslConfig.certPath));
                 InputStream privateKey = new BufferedInputStream(new FileInputStream(sslConfig.privateKeyPath))) {
                builder.keyManager(cert, privateKey);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException(String.format("Invalid certificate or private key: %s", e.getMessage()), e);
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Invalid certificate or private key file path", e);
            } catch (IOException e) {
                throw new RuntimeException("Exception while reading certificate or private key", e);
            }
        } else if ((sslConfig.certPath == null) != (sslConfig.privateKeyPath == null)) {
            throw new RuntimeException(String.format("%s cannot be set without %s and vice-versa: %s is not set",
                    "scylla.ssl.openssl.keyCertChain", "scylla.ssl.openssl.privateKey",
                    (sslConfig.certPath == null) ? "scylla.ssl.openssl.keyCertChain" : "scylla.ssl.openssl.privateKey"));
        }
        try {
            context = builder.build();
        } catch (SSLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SSLEngine newSslEngine(EndPoint remoteEndpoint) {
        SocketAddress address = remoteEndpoint.resolve();
        if (address instanceof InetSocketAddress) {
            InetSocketAddress socketAddress = (InetSocketAddress) address;
            return context.newEngine(ByteBufAllocator.DEFAULT, socketAddress.getHostString(), socketAddress.getPort());
        }
        return context.newEngine(ByteBufAllocator.DEFAULT);
    }

    /**
     * Releases the context, which frees the native memory of an OpenSSL context; a JDK context is
     * not reference-counted, so releasing it does nothing.
     */
    @Override
    public void close() {
        ReferenceCountUtil.release(context);
    }

    private static KeyStore createKeyStore(String path, String password) {
        try {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            try (InputStream inputStream = Files.newInputStream(Paths.get(path))) {
                keyStore.load(inputStream, toCharArray(password));
            } catch (IOException e) {
                throw new RuntimeException("Exception while reading keystore", e);
            } catch (CertificateException | NoSuchAlgorithmException e) {
                throw new RuntimeException("Exception while loading keystore", e);
            }
            return keyStore;
        } catch (KeyStoreException e) {
            throw new RuntimeException("Exception while creating keystore", e);
        }
    }

    private static char[] toCharArray(String password) {
        return password == null ? null : password.toCharArray();
    }
}
