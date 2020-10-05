package com.scylladb.cdc.lib;

import java.util.HashSet;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.scylladb.cdc.model.TableName;

public class Main {


    public static void main(String[] args) {
        try (Cluster c = Cluster.builder().addContactPoint("127.0.0.1").build(); Session s = c.connect()){
            HashSet<TableName> tables = new HashSet<>();
            tables.add(new TableName("ks", "tb"));
            CDCConsumer consumer = CDCConsumerBuilder.builder(s, change -> {
                System.err.println(change.getId());
                CompletableFuture<Void> f = new CompletableFuture<>();
                f.complete(null);
                return f;
            },tables).build();
            consumer.start();
        }
    }

}
