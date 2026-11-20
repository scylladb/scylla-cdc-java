package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.scylladb.cdc.model.FutureUtils;
import com.scylladb.cdc.model.TableName;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

class Driver4CommonCQL {
    private static final CqlIdentifier EXTENSIONS = CqlIdentifier.fromInternal("extensions");
    private static final String SCYLLA_CDC_EXTENSION = "cdc";
    private static final Pattern UNQUOTED_NAME = Pattern.compile("[A-Za-z0-9_]*");

    /**
     * Resolves a keyspace or table name as 3.x {@code Metadata.handleId} does: a name of ASCII
     * letters, digits and underscores is case-insensitive, a double-quoted name is exact, and any
     * other name is taken as-is. Unlike {@link CqlIdentifier#fromCql}, it never throws for names
     * that need quoting, such as {@code order} or {@code 1events}.
     */
    static CqlIdentifier identifier(String name) {
        if (UNQUOTED_NAME.matcher(name).matches()) {
            return CqlIdentifier.fromInternal(name.toLowerCase(Locale.ROOT));
        }
        if (name.length() > 1 && name.charAt(0) == '"' && name.charAt(name.length() - 1) == '"') {
            return CqlIdentifier.fromCql(name);
        }
        return CqlIdentifier.fromInternal(name);
    }

    static Optional<KeyspaceMetadata> keyspace(CqlSession session, TableName table) {
        return session.getMetadata().getKeyspace(identifier(table.keyspace));
    }

    static Optional<TableMetadata> table(KeyspaceMetadata keyspace, TableName table) {
        return keyspace.getTable(identifier(table.name));
    }

    /**
     * Returns the table's raw {@code cdc} extension, or empty if the table does not have ScyllaDB
     * CDC: 4.x has no counterpart of 3.x {@code TableOptionsMetadata.isScyllaCDC()}.
     */
    static Optional<ByteBuffer> cdcExtension(TableMetadata table) {
        @SuppressWarnings("unchecked")
        Map<String, ByteBuffer> extensions = (Map<String, ByteBuffer>) table.getOptions().get(EXTENSIONS);
        return extensions == null ? Optional.empty() : Optional.ofNullable(extensions.get(SCYLLA_CDC_EXTENSION));
    }

    /**
     * Decodes a ScyllaDB map extension, as 3.x {@code MapExtensionReader} does: a little-endian
     * 32-bit entry count, then each key and value as a little-endian 32-bit length and UTF-8 bytes.
     */
    static Map<String, String> parseMapExtension(ByteBuffer raw) {
        ByteBuffer data = raw.slice().order(ByteOrder.LITTLE_ENDIAN);
        int count = data.getInt();
        if (count < 0) {
            throw new IllegalArgumentException("Negative entry count in a map extension: " + count);
        }
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < count; i++) {
            String key = readString(data);
            result.put(key, readString(data));
        }
        return Collections.unmodifiableMap(result);
    }

    private static String readString(ByteBuffer data) {
        byte[] bytes = new byte[data.getInt()];
        data.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    static CompletableFuture<Optional<Long>> fetchTableTTL(CqlSession session, TableName tableName) {
        Optional<KeyspaceMetadata> keyspaceMetadata = keyspace(session, tableName);
        if (!keyspaceMetadata.isPresent()) {
            return FutureUtils.exceptionalFuture(new IllegalArgumentException(
                    String.format("Could not fetch the metadata of keyspace %s.", tableName.keyspace)));
        }

        Optional<TableMetadata> tableMetadata = table(keyspaceMetadata.get(), tableName);
        if (!tableMetadata.isPresent()) {
            return FutureUtils.exceptionalFuture(new IllegalArgumentException(
                    String.format("Could not fetch the metadata of table %s.%s.", tableName.keyspace, tableName.name)));
        }

        Optional<ByteBuffer> cdcExtension = cdcExtension(tableMetadata.get());
        if (!cdcExtension.isPresent()) {
            return FutureUtils.exceptionalFuture(new IllegalArgumentException(
                    String.format("Table %s.%s does not have Scylla CDC enabled.", tableName.keyspace, tableName.name)));
        }
        return decodeTTL(tableName, cdcExtension.get());
    }

    /**
     * Reads the {@code ttl} CDC option from a raw {@code cdc} extension, failing the future if
     * the extension is malformed.
     */
    static CompletableFuture<Optional<Long>> decodeTTL(TableName tableName, ByteBuffer cdcExtension) {
        Map<String, String> cdcOptions;
        try {
            cdcOptions = parseMapExtension(cdcExtension);
        } catch (RuntimeException ex) {
            return FutureUtils.exceptionalFuture(new IllegalArgumentException(
                    String.format("Table %s.%s has a malformed cdc extension.", tableName.keyspace, tableName.name), ex));
        }
        return parseTTL(tableName, cdcOptions);
    }

    /**
     * Reads the {@code ttl} CDC option; {@code 0} means the CDC log has no TTL.
     */
    static CompletableFuture<Optional<Long>> parseTTL(TableName tableName, Map<String, String> cdcOptions) {
        String ttl = cdcOptions.get("ttl");
        if (ttl == null) {
            return FutureUtils.exceptionalFuture(new IllegalArgumentException(
                    String.format("Table %s.%s does not have a TTL value in its metadata, " +
                            "even though Scylla CDC is enabled and the metadata is present.", tableName.keyspace, tableName.name)));
        }

        try {
            long parsedTTL = Long.parseLong(ttl);
            if (parsedTTL == 0) {
                return CompletableFuture.completedFuture(Optional.empty());
            } else {
                return CompletableFuture.completedFuture(Optional.of(parsedTTL));
            }
        } catch (NumberFormatException ex) {
            return FutureUtils.exceptionalFuture(new IllegalArgumentException(
                    String.format("Table %s.%s has invalid TTL value: %s.", tableName.keyspace, tableName.name, ttl)));
        }
    }
}
