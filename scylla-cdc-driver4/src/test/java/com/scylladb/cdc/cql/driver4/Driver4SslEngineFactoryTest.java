package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.scylladb.cdc.cql.SslConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLEngine;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Driver4SslEngineFactoryTest {

    private static SslConfig emptySslConfig() {
        return new SslConfig(null, null, null, null, null, null, null, null);
    }

    @Test
    public void testEngineDoesNotValidateHostNames() {
        Driver4SslEngineFactory factory = new Driver4SslEngineFactory(emptySslConfig());

        SSLEngine engine = factory.newSslEngine(new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9142)));

        assertTrue(engine.getUseClientMode());
        assertEquals("127.0.0.1", engine.getPeerHost());
        assertEquals(9142, engine.getPeerPort());
        assertNull(engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @Test
    public void testTrustStoreWithoutPasswordIsLoaded(@TempDir Path directory) throws Exception {
        Path trustStore = directory.resolve("truststore.jks");
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, null);
        try (OutputStream output = Files.newOutputStream(trustStore)) {
            keyStore.store(output, "secret".toCharArray());
        }
        SslConfig sslConfig = new SslConfig(null, trustStore.toString(), null, null, null, null, null, null);

        assertDoesNotThrow(() -> new Driver4SslEngineFactory(sslConfig));
    }

    @Test
    public void testInvalidCertificateKeepsTheCause(@TempDir Path directory) throws Exception {
        Path cert = Files.write(directory.resolve("cert.pem"), "not a certificate".getBytes(StandardCharsets.US_ASCII));
        Path key = Files.write(directory.resolve("key.pem"), "not a key".getBytes(StandardCharsets.US_ASCII));
        SslConfig sslConfig = new SslConfig(null, null, null, null, null, null, cert.toString(), key.toString());

        RuntimeException e = assertThrows(RuntimeException.class, () -> new Driver4SslEngineFactory(sslConfig));
        assertTrue(e.getMessage().startsWith("Invalid certificate or private key"), e.getMessage());
        assertInstanceOf(IllegalArgumentException.class, e.getCause());
    }

    @Test
    public void testCertificateWithoutPrivateKeyIsRejected() {
        SslConfig sslConfig = new SslConfig(null, null, null, null, null, null, "cert.pem", null);

        RuntimeException e = assertThrows(RuntimeException.class, () -> new Driver4SslEngineFactory(sslConfig));
        assertTrue(e.getMessage().contains("scylla.ssl.openssl.privateKey is not set"), e.getMessage());
    }
}
