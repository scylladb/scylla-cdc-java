package com.scylladb.cdc.cql;

import java.util.ArrayList;
import java.util.List;

public class SslConfig {
    public final String sslProviderString;
    public final String trustStorePath;
    public final String trustStorePassword;
    public final String keyStorePath;
    public final String keyStorePassword;
    public final List<String> cipherSuites;
    public final String certPath;
    public final String privateKeyPath;

    public SslConfig(String sslProviderString, String trustStorePath, String trustStorePassword, String keyStorePath, String keyStorePassword, List<String> cipherSuites, String certPath, String privateKeyPath) {
        this.sslProviderString = sslProviderString;
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
        this.cipherSuites = cipherSuites;
        this.certPath = certPath;
        this.privateKeyPath = privateKeyPath;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String sslProviderString = null;
        private String trustStorePath = null;
        private String trustStorePassword = null;
        private String keyStorePath = null;
        private String keyStorePassword = null;
        private final List<String> cipherSuites = new ArrayList<>();
        private String certPath = null;
        private String privateKeyPath = null;

        public Builder withSslProviderString(String sslProviderString) {
            this.sslProviderString = sslProviderString;
            return this;
        }

        public Builder withTrustStorePath(String trustStorePath) {
            this.trustStorePath = trustStorePath;
            return this;
        }

        public Builder withTrustStorePassword(String trustStorePassword) {
            this.trustStorePassword = trustStorePassword;
            return this;
        }

        public Builder withKeyStorePath(String keyStorePath) {
            this.keyStorePath = keyStorePath;
            return this;
        }

        public Builder withKeyStorePassword(String keyStorePassword) {
            this.keyStorePassword = keyStorePassword;
            return this;
        }

        public Builder withCipher(String cipher) {
            this.cipherSuites.add(cipher);
            return this;
        }

        public Builder withCertPath(String certPath) {
            this.certPath = certPath;
            return this;
        }

        public Builder withPrivateKeyPath(String privateKeyPath) {
            this.privateKeyPath = privateKeyPath;
            return this;
        }

        public SslConfig build() {
            return new SslConfig(sslProviderString, trustStorePath, trustStorePassword, keyStorePath, keyStorePassword, cipherSuites, certPath, privateKeyPath);
        }
    }
}
