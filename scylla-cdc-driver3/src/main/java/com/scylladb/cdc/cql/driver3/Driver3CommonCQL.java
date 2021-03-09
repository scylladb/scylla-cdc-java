package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.scylladb.cdc.model.FutureUtils;
import com.scylladb.cdc.model.TableName;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

class Driver3CommonCQL {
    protected static CompletableFuture<Optional<Long>> fetchTableTTL(Session session, TableName tableName) {
        Metadata metadata = session.getCluster().getMetadata();
        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(tableName.keyspace);
        if (keyspaceMetadata == null) {
            return FutureUtils.exceptionalFuture(new IllegalArgumentException(
                    String.format("Could not fetch the metadata of keyspace %s.", tableName.keyspace)));
        }

        TableMetadata tableMetadata = keyspaceMetadata.getTable(tableName.name);
        if (tableMetadata == null) {
            return FutureUtils.exceptionalFuture(new IllegalArgumentException(
                    String.format("Could not fetch the metadata of table %s.%s.", tableName.keyspace, tableName.name)));
        }

        if (!tableMetadata.getOptions().isScyllaCDC()) {
            return FutureUtils.exceptionalFuture(new IllegalArgumentException(
                    String.format("Table %s.%s does not have Scylla CDC enabled.", tableName.keyspace, tableName.name)));
        }

        Map<String, String> scyllaCDCOptions = tableMetadata.getOptions().getScyllaCDCOptions();
        if (scyllaCDCOptions == null) {
            return FutureUtils.exceptionalFuture(new IllegalArgumentException(
                    String.format("Table %s.%s does not have Scylla CDC metadata, " +
                            "even though CDC is enabled.", tableName.keyspace, tableName.name)));
        }

        String ttl = scyllaCDCOptions.get("ttl");
        if (ttl == null) {
            return FutureUtils.exceptionalFuture(new IllegalArgumentException(
                    String.format("Table %s.%s does not have a TTL value in its metadata, " +
                            "even though Scylla CDC is enabled and the metadata is present.", tableName.keyspace, tableName.name)));
        }

        try {
            long parsedTTL = Long.parseLong(ttl);
            if (parsedTTL == 0) {
                // TTL is disabled.
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
