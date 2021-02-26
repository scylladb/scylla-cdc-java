package com.scylladb.cdc.cql;

import com.google.common.base.Preconditions;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CQLConfiguration {
    private static final int DEFAULT_PORT = 9042;

    public final List<InetSocketAddress> contactPoints;
    public final String user;
    public final String password;

    private CQLConfiguration(List<InetSocketAddress> contactPoints,
                            String user, String password) {
        this.contactPoints = Preconditions.checkNotNull(contactPoints);
        Preconditions.checkArgument(!contactPoints.isEmpty());

        this.user = user;
        this.password = password;
        // Either someone did not provide credentials
        // or provided user-password pair.
        Preconditions.checkArgument((this.user == null && this.password == null)
                || (this.user != null && this.password != null));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<InetSocketAddress> contactPoints = new ArrayList<>();
        private String user = null;
        private String password = null;

        public Builder addContactPoint(InetSocketAddress contactPoint) {
            Preconditions.checkNotNull(contactPoint);
            contactPoints.add(contactPoint);
            return this;
        }

        public Builder addContactPoints(Collection<InetSocketAddress> addedContactPoints) {
            for (InetSocketAddress contactPoint : addedContactPoints) {
                this.addContactPoint(contactPoint);
            }
            return this;
        }

        public Builder addContactPoint(String host, int port) {
            Preconditions.checkNotNull(host);
            Preconditions.checkArgument(port > 0 && port < 65536);
            return addContactPoint(new InetSocketAddress(host, port));
        }

        public Builder addContactPoint(String host) {
            return addContactPoint(host, DEFAULT_PORT);
        }

        public Builder withCredentials(String user, String password) {
            this.user = Preconditions.checkNotNull(user);
            this.password = Preconditions.checkNotNull(password);
            return this;
        }

        public CQLConfiguration build() {
            return new CQLConfiguration(contactPoints, user, password);
        }
    }
}