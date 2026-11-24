package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.scylladb.cdc.cql.SslConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Driver4SslEngineFactoryTest {
    private static final String PASSWORD = "secret";
    private static final String CIPHER = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256";

    @TempDir
    static Path keys;
    private static Path clientKeyStore;
    private static Path serverKeyStore;
    private static Path clientTrustStore;

    /**
     * Generates self-signed client and server keys with the JDK's keytool, so no keys are checked in.
     */
    @BeforeAll
    public static void generateKeys() throws Exception {
        clientKeyStore = generateKeyStore("client");
        serverKeyStore = generateKeyStore("server");
        clientTrustStore = trustStore("client-trust", certificate(serverKeyStore, "server"));
    }

    private static SslConfig emptySslConfig() {
        return new SslConfig(null, null, null, null, null, null, null, null);
    }

    private static SSLEngine newEngine(Driver4SslEngineFactory factory) {
        return factory.newSslEngine(new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9142)));
    }

    @Test
    public void testEngineDoesNotValidateHostNames() {
        SSLEngine engine = newEngine(new Driver4SslEngineFactory(emptySslConfig()));

        assertTrue(engine.getUseClientMode());
        assertEquals("127.0.0.1", engine.getPeerHost());
        assertEquals(9142, engine.getPeerPort());
        assertNull(engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @Test
    public void testProviderIsTakenFromTheConfiguration() {
        assertDoesNotThrow(() -> new Driver4SslEngineFactory(new SslConfig("JDK", null, null, null, null, null, null, null)));
        assertThrows(IllegalArgumentException.class,
                () -> new Driver4SslEngineFactory(new SslConfig("NO_SUCH_PROVIDER", null, null, null, null, null, null, null)));
    }

    @Test
    public void testCipherSuitesAreRestricted() {
        SSLEngine engine = newEngine(new Driver4SslEngineFactory(
                new SslConfig(null, null, null, null, null, Collections.singletonList(CIPHER), null, null)));
        SSLEngine defaultEngine = newEngine(new Driver4SslEngineFactory(emptySslConfig()));

        String[] enabled = engine.getEnabledCipherSuites();
        assertTrue(Arrays.asList(enabled).contains(CIPHER), Arrays.toString(enabled));
        assertTrue(enabled.length < defaultEngine.getEnabledCipherSuites().length, Arrays.toString(enabled));
    }

    @Test
    public void testTrustStoreWithoutPasswordIsLoaded(@TempDir Path directory) throws Exception {
        Path trustStore = store(emptyKeyStore(), directory.resolve("truststore.jks"));
        SslConfig sslConfig = new SslConfig(null, trustStore.toString(), null, null, null, null, null, null);

        assertDoesNotThrow(() -> new Driver4SslEngineFactory(sslConfig));
    }

    @Test
    public void testKeyStoreWithoutPasswordIsLoaded(@TempDir Path directory) throws Exception {
        Path keyStore = store(emptyKeyStore(), directory.resolve("keystore.jks"));
        SslConfig sslConfig = new SslConfig(null, null, null, keyStore.toString(), null, null, null, null);

        assertDoesNotThrow(() -> new Driver4SslEngineFactory(sslConfig));
    }

    @Test
    public void testKeyStoreAuthenticatesTheClient() throws Exception {
        SslConfig sslConfig = new SslConfig(null, clientTrustStore.toString(), PASSWORD,
                clientKeyStore.toString(), PASSWORD, null, null, null);

        assertClientAuthenticated(new Driver4SslEngineFactory(sslConfig));
    }

    @Test
    public void testPemCertificateAndKeyAuthenticateTheClient(@TempDir Path directory) throws Exception {
        KeyStore keyStore = load(clientKeyStore);
        Path cert = Files.write(directory.resolve("cert.pem"),
                pem("CERTIFICATE", keyStore.getCertificate("client").getEncoded()));
        Path key = Files.write(directory.resolve("key.pem"),
                pem("PRIVATE KEY", keyStore.getKey("client", PASSWORD.toCharArray()).getEncoded()));
        SslConfig sslConfig = new SslConfig(null, clientTrustStore.toString(), PASSWORD,
                null, null, null, cert.toString(), key.toString());

        assertClientAuthenticated(new Driver4SslEngineFactory(sslConfig));
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

    /**
     * Runs an in-memory handshake with a server that trusts only the client's certificate and
     * requires one, and checks that the server received it.
     */
    private static void assertClientAuthenticated(Driver4SslEngineFactory factory) throws Exception {
        KeyManagerFactory keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagers.init(load(serverKeyStore), PASSWORD.toCharArray());
        KeyStore serverTrust = emptyKeyStore();
        serverTrust.setCertificateEntry("client", certificate(clientKeyStore, "client"));
        TrustManagerFactory trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagers.init(serverTrust);
        SSLContext serverContext = SSLContext.getInstance("TLS");
        serverContext.init(keyManagers.getKeyManagers(), trustManagers.getTrustManagers(), null);
        SSLEngine server = serverContext.createSSLEngine();
        server.setUseClientMode(false);
        server.setNeedClientAuth(true);

        handshake(newEngine(factory), server);

        Certificate[] peer = server.getSession().getPeerCertificates();
        assertArrayEquals(certificate(clientKeyStore, "client").getEncoded(), peer[0].getEncoded());
    }

    private static void handshake(SSLEngine client, SSLEngine server) throws SSLException {
        ByteBuffer empty = ByteBuffer.allocate(0);
        ByteBuffer toServer = ByteBuffer.allocate(client.getSession().getPacketBufferSize());
        ByteBuffer toClient = ByteBuffer.allocate(server.getSession().getPacketBufferSize());
        ByteBuffer clientData = ByteBuffer.allocate(client.getSession().getApplicationBufferSize());
        ByteBuffer serverData = ByteBuffer.allocate(server.getSession().getApplicationBufferSize());
        client.beginHandshake();
        server.beginHandshake();
        for (int i = 0; i < 100 && !(done(client) && done(server)); i++) {
            client.wrap(empty, toServer);
            runTasks(client);
            server.wrap(empty, toClient);
            runTasks(server);
            toServer.flip();
            toClient.flip();
            client.unwrap(toClient, clientData);
            runTasks(client);
            server.unwrap(toServer, serverData);
            runTasks(server);
            toServer.compact();
            toClient.compact();
        }
        assertTrue(done(client) && done(server), "the handshake finished");
    }

    private static boolean done(SSLEngine engine) {
        HandshakeStatus status = engine.getHandshakeStatus();
        return status == HandshakeStatus.NOT_HANDSHAKING || status == HandshakeStatus.FINISHED;
    }

    private static void runTasks(SSLEngine engine) {
        Runnable task;
        while ((task = engine.getDelegatedTask()) != null) {
            task.run();
        }
    }

    private static Path generateKeyStore(String alias) throws Exception {
        Path keyStore = keys.resolve(alias + ".jks");
        File log = keys.resolve(alias + ".log").toFile();
        String keytool = Paths.get(System.getProperty("java.home"), "bin", "keytool").toString();
        Process process = new ProcessBuilder(keytool, "-genkeypair", "-alias", alias, "-keyalg", "RSA",
                "-keysize", "2048", "-dname", "CN=" + alias, "-validity", "2", "-storetype", "JKS",
                "-keystore", keyStore.toString(), "-storepass", PASSWORD, "-keypass", PASSWORD)
                .redirectErrorStream(true)
                .redirectOutput(log)
                .start();
        try {
            assertTrue(process.waitFor(60, TimeUnit.SECONDS), "keytool finished");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } finally {
            destroy(process);
        }
        assertEquals(0, process.exitValue(),
                () -> "keytool failed: " + new String(readAll(log.toPath()), StandardCharsets.UTF_8));
        return keyStore;
    }

    /**
     * Kills a process that is still running and waits for it to exit, keeping the interrupt flag.
     */
    private static void destroy(Process process) {
        if (!process.isAlive()) {
            return;
        }
        process.destroyForcibly();
        boolean interrupted = Thread.interrupted();
        try {
            process.waitFor(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            interrupted = true;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static byte[] readAll(Path path) {
        try {
            return Files.readAllBytes(path);
        } catch (Exception e) {
            return new byte[0];
        }
    }

    private static Path trustStore(String name, Certificate certificate) throws Exception {
        KeyStore keyStore = emptyKeyStore();
        keyStore.setCertificateEntry("trusted", certificate);
        return store(keyStore, keys.resolve(name + ".jks"));
    }

    private static KeyStore emptyKeyStore() throws Exception {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, null);
        return keyStore;
    }

    private static Path store(KeyStore keyStore, Path path) throws Exception {
        try (OutputStream output = Files.newOutputStream(path)) {
            keyStore.store(output, PASSWORD.toCharArray());
        }
        return path;
    }

    private static KeyStore load(Path path) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        try (InputStream input = Files.newInputStream(path)) {
            keyStore.load(input, PASSWORD.toCharArray());
        }
        return keyStore;
    }

    private static Certificate certificate(Path keyStore, String alias) throws Exception {
        return load(keyStore).getCertificate(alias);
    }

    private static byte[] pem(String type, byte[] der) {
        String body = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der);
        return ("-----BEGIN " + type + "-----\n" + body + "\n-----END " + type + "-----\n")
                .getBytes(StandardCharsets.US_ASCII);
    }
}
