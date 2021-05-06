package com.scylladb.cdc.lib;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.cql.driver3.Driver3MasterCQL;
import com.scylladb.cdc.cql.driver3.Driver3Session;
import com.scylladb.cdc.cql.driver3.Driver3WorkerCQL;
import com.scylladb.cdc.model.RetryBackoff;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.master.MasterConfiguration;
import com.scylladb.cdc.model.worker.Consumer;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import com.scylladb.cdc.model.worker.TaskAndRawChangeConsumer;
import com.scylladb.cdc.model.worker.WorkerConfiguration;

public final class CDCConsumer implements AutoCloseable {

    private final LocalTransport transport;
    private final ThreadGroup cdcThreadGroup;
    private final Driver3Session session;
    private final MasterConfiguration masterConfiguration;
    private MasterThread master;

    private CDCConsumer(CQLConfiguration cqlConfiguration, MasterConfiguration.Builder masterConfigurationBuilder,
                       WorkerConfiguration.Builder workerConfigurationBuilder) {
        Preconditions.checkNotNull(cqlConfiguration);
        Preconditions.checkNotNull(masterConfigurationBuilder);
        Preconditions.checkNotNull(workerConfigurationBuilder);

        this.cdcThreadGroup = new ThreadGroup("Scylla-CDC-Threads");

        this.session = new Driver3Session(cqlConfiguration);
        workerConfigurationBuilder.withCQL(new Driver3WorkerCQL(session));
        this.transport = new LocalTransport(cdcThreadGroup, workerConfigurationBuilder);

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

        private int workersCount = getDefaultWorkersCount();
        private ScheduledExecutorService executorService;

        @SuppressWarnings("deprecation")
        public Builder withConsumerProvider(RawChangeConsumerProvider consumerProvider) {
            withConsumer(consumerProvider.getForThread(0));
            return this;
        }
        
        public Builder withConsumer(Consumer consumer) {
            workerConfigurationBuilder.withConsumer(consumer);
            return this;
        }

        public Builder withConsumer(TaskAndRawChangeConsumer consumer) {
            workerConfigurationBuilder.withConsumer(consumer);
            return this;
        }

        public Builder withTaskAndRawChangeConsumer(TaskAndRawChangeConsumer consumer) {
            workerConfigurationBuilder.withTaskAndRawChangeConsumer(consumer);
            return this;
        }

        public Builder withConsumer(RawChangeConsumer consumer) {
            workerConfigurationBuilder.withConsumer(consumer);
            return this;
        }

        public Builder withRawChangeConsumer(RawChangeConsumer consumer) {
            workerConfigurationBuilder.withRawChangeConsumer(consumer);
            return this;
        }

        public Builder addTable(TableName table) {
            masterConfigurationBuilder.addTable(table);
            return this;
        }

        public Builder addTable(String keyspace, String table) {
            return addTable(new TableName(keyspace, table));
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

        /**
         * Sets the executor service to use for task scheduling.
         *
         * Note: CDC library uses delayed/scheduled operations in some cases.
         * To ensure the fastest possible terminaton/shutdown of a running
         * worker, you should utilize a {@link ScheduledExecutorService} that
         * does not wait for non-started tasks scheduled in the future
         * when shutting down.
         *
         * The default exeutor uses a {@link ScheduledThreadPoolExecutor}
         * with {@link ScheduledThreadPoolExecutor#setExecuteExistingDelayedTasksAfterShutdownPolicy(boolean)}
         * set to false
         *
         * @see ScheduledThreadPoolExecutor
         * @see ScheduledThreadPoolExecutor#setExecuteExistingDelayedTasksAfterShutdownPolicy(boolean)
         *
         * @param executorService
         * @return
         */
        public Builder withExecutorService(ScheduledExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        public CDCConsumer build() {
            if (executorService == null) {
                final ScheduledThreadPoolExecutor s = new ScheduledThreadPoolExecutor(
                        workersCount);
                s.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
                executorService = s;
            }
            workerConfigurationBuilder.withExecutorService(executorService);
            return new CDCConsumer(cqlConfigurationBuilder.build(),
                    masterConfigurationBuilder, workerConfigurationBuilder);
        }

        private static int getDefaultWorkersCount() {
            int result = Runtime.getRuntime().availableProcessors() - 1;
            return result > 0 ? result : 1;
        }
    }
}
