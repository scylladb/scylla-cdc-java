package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.scylladb.cdc.model.TableName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Driver4CommonCQLTest {
    private static final TableName TABLE = new TableName("ks", "t");

    /**
     * Encodes entries the way ScyllaDB stores a map extension.
     */
    private static ByteBuffer encode(String... keysAndValues) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(out, keysAndValues.length / 2);
        for (String s : keysAndValues) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            writeInt(out, bytes.length);
            out.write(bytes, 0, bytes.length);
        }
        return ByteBuffer.wrap(out.toByteArray());
    }

    private static void writeInt(ByteArrayOutputStream out, int value) {
        byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
        out.write(bytes, 0, bytes.length);
    }

    @Test
    public void testParseMapExtensionDecodesCdcOptions() {
        Map<String, String> expected = new HashMap<>();
        expected.put("enabled", "true");
        expected.put("ttl", "86400");
        expected.put("delta", "full");
        expected.put("\u00fcn\u00efcode", "v\u00e4lue");

        ByteBuffer raw = encode("enabled", "true", "ttl", "86400", "delta", "full", "\u00fcn\u00efcode", "v\u00e4lue");

        assertEquals(expected, Driver4CommonCQL.parseMapExtension(raw));
        // The metadata's buffer is shared, so decoding must not move its position.
        assertEquals(0, raw.position());
        assertEquals(expected, Driver4CommonCQL.parseMapExtension(raw));
    }

    @Test
    public void testParseMapExtensionDecodesEmptyMap() {
        assertEquals(Collections.emptyMap(), Driver4CommonCQL.parseMapExtension(encode()));
    }

    @Test
    public void testParseMapExtensionRejectsNegativeCount() {
        ByteBuffer raw = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0, -1);
        assertThrows(IllegalArgumentException.class, () -> Driver4CommonCQL.parseMapExtension(raw));
    }

    @Test
    public void testIdentifierResolvesNamesAs3xDoes() {
        // Expected internal forms are those of 3.x Metadata.handleId.
        assertEquals(CqlIdentifier.fromInternal("events"), Driver4CommonCQL.identifier("events"));
        assertEquals(CqlIdentifier.fromInternal("order"), Driver4CommonCQL.identifier("order"));
        assertEquals(CqlIdentifier.fromInternal("_audit"), Driver4CommonCQL.identifier("_audit"));
        assertEquals(CqlIdentifier.fromInternal("1events"), Driver4CommonCQL.identifier("1events"));
        assertEquals(CqlIdentifier.fromInternal("events"), Driver4CommonCQL.identifier("Events"));
        assertEquals(CqlIdentifier.fromInternal("Events"), Driver4CommonCQL.identifier("\"Events\""));
        assertEquals(CqlIdentifier.fromInternal("a\"b"), Driver4CommonCQL.identifier("\"a\"\"b\""));
        assertEquals(CqlIdentifier.fromInternal("my-table"), Driver4CommonCQL.identifier("my-table"));
        assertEquals(CqlIdentifier.fromInternal("\"half"), Driver4CommonCQL.identifier("\"half"));
    }

    @Test
    public void testDecodeTTL() throws Exception {
        assertEquals(Optional.of(3600L), Driver4CommonCQL.decodeTTL(TABLE, encode("enabled", "true", "ttl", "3600")).get());
    }

    @Test
    public void testDecodeTTLFailsOnMalformedExtension() {
        ByteBuffer truncated = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0, 1);
        assertFailsWith(Driver4CommonCQL.decodeTTL(TABLE, truncated), "malformed cdc extension");
    }

    @Test
    public void testDecodeTTLFailsOnInvalidStringLength() {
        for (int length : new int[] {Integer.MAX_VALUE, -1}) {
            ByteBuffer raw = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putInt(0, 1).putInt(4, length);
            ExecutionException e = assertThrows(ExecutionException.class, Driver4CommonCQL.decodeTTL(TABLE, raw)::get);
            assertEquals("Invalid string length in a map extension: " + length, e.getCause().getCause().getMessage());
        }
    }

    @Test
    public void testParseTTL() throws Exception {
        assertEquals(Optional.of(86400L),
                Driver4CommonCQL.parseTTL(TABLE, Collections.singletonMap("ttl", "86400")).get());
        assertEquals(Optional.empty(),
                Driver4CommonCQL.parseTTL(TABLE, Collections.singletonMap("ttl", "0")).get());
    }

    @Test
    public void testParseTTLFailsWithoutTTL() {
        assertFailsWith(Driver4CommonCQL.parseTTL(TABLE, Collections.singletonMap("enabled", "true")),
                "does not have a TTL value");
    }

    @Test
    public void testParseTTLFailsOnInvalidTTL() {
        assertFailsWith(Driver4CommonCQL.parseTTL(TABLE, Collections.singletonMap("ttl", "a day")),
                "invalid TTL value: a day");
    }

    private static void assertFailsWith(CompletableFuture<?> future, String message) {
        ExecutionException e = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(IllegalArgumentException.class, e.getCause());
        assertTrue(e.getCause().getMessage().contains(message), e.getCause().getMessage());
    }
}
