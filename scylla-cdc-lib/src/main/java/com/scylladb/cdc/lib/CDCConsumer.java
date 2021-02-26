package com.scylladb.cdc.lib;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.cql.driver3.Driver3MasterCQL;
import com.scylladb.cdc.cql.driver3.Driver3Session;
import com.scylladb.cdc.cql.driver3.Driver3WorkerCQL;
import com.scylladb.cdc.model.ExponentialRetryBackoffWithJitter;
import com.scylladb.cdc.model.RetryBackoff;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.master.Master;
import com.scylladb.cdc.model.master.MasterConfiguration;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import com.scylladb.cdc.model.worker.TaskAndRawChangeConsumer;
import com.scylladb.cdc.model.worker.Worker;
import com.scylladb.cdc.model.worker.WorkerConfiguration;

public final class CDCConsumer implements AutoCloseable {

    private final LocalTransport transport;
    private final ThreadGroup cdcThreadGroup;
    private final Driver3Session session;
    private final MasterConfiguration masterConfiguration;
    private MasterThread master;

    private CDCConsumer(CQLConfiguration cqlConfiguration, MasterConfiguration.Builder masterConfigurationBuilder,
                       WorkerConfiguration.Builder workerConfigurationBuilder,
                       RawChangeConsumerProvider consumerProvider, int workersCount) {
        Preconditions.checkNotNull(cqlConfiguration);
        Preconditions.checkNotNull(masterConfigurationBuilder);
        Preconditions.checkNotNull(workerConfigurationBuilder);
        Preconditions.checkNotNull(consumerProvider);

        this.cdcThreadGroup = new ThreadGroup("Scylla-CDC-Threads");
        Preconditions.checkArgument(workersCount > 0);

        this.session = new Driver3Session(cqlConfiguration);
        workerConfigurationBuilder.withCQL(new Driver3WorkerCQL(session));
        this.transport = new LocalTransport(cdcThreadGroup, workersCount,
                workerConfigurationBuilder, consumerProvider);

        MasterCQL masterCQL = new Driver3MasterCQL(session);
        this.masterConfiguration = masterConfigurationBuilder
                .withCQL(masterCQL)
                .withTransport(transport).build();
    }

    public void start() {
        Preconditions.checkState(master == null);
        Preconditions.checkState(transport.isReadyToStart());
        master = new MasterThread(cdcThreadGroup, masterConfiguration);
        master.start();
    }

    public Optional<Throwable> validate() {
        Preconditions.checkState(master == null);

        // Create a "probe" master.
        master = new MasterThread(cdcThreadGroup, masterConfiguration);

        // Validate it.
        Optional<Throwable> validationResult = master.validate();

        // Dispose of a "probe" master.
        master = null;
        return validationResult;
    }

    public void stop() throws InterruptedException {
        if (master != null) {
            master.finish();
        }
        master = null;
        if (transport != null) {
            transport.stop();
        }
        if (session != null) {
            session.close();
        }
    }

    public void reconfigure(int workersCount) throws InterruptedException {
        Preconditions.checkArgument(workersCount > 0);
        stop();
        transport.setWorkersCount(workersCount);
        start();
    }

    @Override
    public void close() throws InterruptedException {
        this.stop();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final CQLConfiguration.Builder cqlConfigurationBuilder
                = CQLConfiguration.builder();
        private final MasterConfiguration.Builder masterConfigurationBuilder
                = MasterConfiguration.builder();
        private final WorkerConfiguration.Builder workerConfigurationBuilder
                = WorkerConfiguration.builder();

        private RawChangeConsumerProvider consumerProvider;
        private int workersCount = getDefaultWorkersCount();

        public Builder withConsumerProvider(RawChangeConsumerProvider consumerProvider) {
            this.consumerProvider = Preconditions.checkNotNull(consumerProvider);
            return this;
        }

        public Builder addTable(TableName table) {
            masterConfigurationBuilder.addTable(table);
            return this;
        }

        public Builder addTables(Collection<TableName> tables) {
            masterConfigurationBuilder.addTables(tables);
            return this;
        }

        public Builder withWorkersCount(int workersCount) {
            Preconditions.checkArgument(workersCount > 0);
            this.workersCount = workersCount;
            return this;
        }

        public Builder withQueryTimeWindowSizeMs(long queryTimeWindowSizeMs) {
            workerConfigurationBuilder.withQueryTimeWindowSizeMs(queryTimeWindowSizeMs);
            return this;
        }

        public Builder withConfidenceWindowSizeMs(long confidenceWindowSizeMs) {
            workerConfigurationBuilder.withConfidenceWindowSizeMs(confidenceWindowSizeMs);
            return this;
        }

        public Builder withWorkerRetryBackoff(RetryBackoff workerRetryBackoff) {
            workerConfigurationBuilder.withWorkerRetryBackoff(workerRetryBackoff);
            return this;
        }

        public Builder addContactPoint(InetSocketAddress contactPoint) {
            cqlConfigurationBuilder.addContactPoint(contactPoint);
            return this;
        }

        public Builder addContactPoints(Collection<InetSocketAddress> contactPoints) {
            cqlConfigurationBuilder.addContactPoints(contactPoints);
            return this;
        }

        public Builder addContactPoint(String host) {
            cqlConfigurationBuilder.addContactPoint(host);
            return this;
        }

        public Builder addContactPoint(String host, int port) {
            cqlConfigurationBuilder.addContactPoint(host, port);
            return this;
        }

        public Builder withCredentials(String user, String password) {
            cqlConfigurationBuilder.withCredentials(user, password);
            return this;
        }

        public Builder withSleepBeforeFirstGenerationMs(long sleepBeforeFirstGenerationMs) {
            masterConfigurationBuilder.withSleepBeforeFirstGenerationMs(sleepBeforeFirstGenerationMs);
            return this;
        }

        public Builder withSleepBeforeGenerationDoneMs(long sleepBeforeGenerationDoneMs) {
            masterConfigurationBuilder.withSleepBeforeFirstGenerationMs(sleepBeforeGenerationDoneMs);
            return this;
        }

        public Builder withSleepAfterExceptionMs(long sleepAfterExceptionMs) {
            masterConfigurationBuilder.withSleepAfterExceptionMs(sleepAfterExceptionMs);
            return this;
        }

        private static int getDefaultWorkersCount() {
            int result = Runtime.getRuntime().availableProcessors() - 1;
            return result > 0 ? result : 1;
        }

        public CDCConsumer build() {
            return new CDCConsumer(cqlConfigurationBuilder.build(),
                    masterConfigurationBuilder, workerConfigurationBuilder,
                    consumerProvider, workersCount);
        }
    }
}
