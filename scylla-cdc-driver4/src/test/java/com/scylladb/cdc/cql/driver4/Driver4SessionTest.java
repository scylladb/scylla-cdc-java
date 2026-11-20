package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.cql.SslConfig;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class Driver4SessionTest {

    @Test
    public void testUnsupportedSettingsAreLoggedAsWarnings() {
        // Nothing listens on port 1, so building the session fails after the warnings are logged.
        CQLConfiguration configuration = CQLConfiguration.builder()
                .addContactPoint("127.0.0.1", 1)
                .withPoolingMaxQueueSize(10)
                .build();
        List<LogRecord> records = new CopyOnWriteArrayList<>();
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                records.add(record);
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        Logger logger = Logger.getLogger(Driver4Session.class.getName());
        logger.addHandler(handler);
        try {
            assertThrows(AllNodesFailedException.class, () -> new Driver4Session(configuration));
        } finally {
            logger.removeHandler(handler);
        }

        List<String> warnings = records.stream()
                .filter(record -> record.getLevel() == Level.WARNING)
                .map(LogRecord::getMessage)
                .collect(Collectors.toList());
        List<String> expected = Driver4Configuration.unsupportedSettings(configuration);
        assertEquals(2, expected.size(), expected.toString());
        assertEquals(expected, warnings);
    }

    @Test
    public void testCredentialsAndSslArePassedToTheBuilder() {
        CQLConfiguration configuration = CQLConfiguration.builder()
                .addContactPoint("127.0.0.1", 9042)
                .withCredentials("cdc_user", "cdc_password")
                .withSslConfig(new SslConfig(null, null, null, null, null, null, null, null))
                .build();
        RecordingBuilder builder = new RecordingBuilder();

        assertSame(builder, Driver4Session.configure(builder, configuration));

        assertEquals(Arrays.asList("cdc_user", "cdc_password"), builder.credentials);
        assertInstanceOf(Driver4SslEngineFactory.class, builder.sslEngineFactory);
    }

    @Test
    public void testNoCredentialsOrSslByDefault() {
        CQLConfiguration configuration = CQLConfiguration.builder().addContactPoint("127.0.0.1", 9042).build();
        RecordingBuilder builder = new RecordingBuilder();

        Driver4Session.configure(builder, configuration);

        assertNull(builder.credentials);
        assertNull(builder.sslEngineFactory);
    }

    /**
     * Records what {@link Driver4Session#configure} passes to the driver's builder.
     */
    private static final class RecordingBuilder extends CqlSessionBuilder {
        private List<String> credentials;
        private SslEngineFactory sslEngineFactory;

        @Override
        public CqlSessionBuilder withAuthCredentials(String username, String password) {
            credentials = Arrays.asList(username, password);
            return super.withAuthCredentials(username, password);
        }

        @Override
        public CqlSessionBuilder withSslEngineFactory(SslEngineFactory sslEngineFactory) {
            this.sslEngineFactory = sslEngineFactory;
            return super.withSslEngineFactory(sslEngineFactory);
        }
    }
}
