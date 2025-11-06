package com.scylladb.cdc.lib;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.cql.WorkerCQL;
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
                       WorkerConfiguration.Builder workerConfigurationBuilder, Supplier<ScheduledExecutorService> executorServiceSupplier,
                       Function<Driver3Session, WorkerCQL> workerCQLProvider) {
        Preconditions.checkNotNull(cqlConfiguration);
        Preconditions.checkNotNull(masterConfigurationBuilder);
        Preconditions.checkNotNull(workerConfigurationBuilder);
        Preconditions.checkNotNull(workerCQLProvider);

        this.cdcThreadGroup = new ThreadGroup("Scylla-CDC-Threads");

        this.session = new Driver3Session(cqlConfiguration);
        workerConfigurationBuilder.withCQL(workerCQLProvider.apply(session));
        this.transport = new LocalTransport(cdcThreadGroup, workerConfigurationBuilder, executorServiceSupplier);

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
        private Supplier<ScheduledExecutorService> executorServiceSupplier = getDefaultExecutorServiceSupplier(workersCount);
        private Function<Driver3Session, WorkerCQL> workerCQLProvider = Driver3WorkerCQL::new;

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

        /**
         * Sets the size of the query time window. Smaller size means smaller queries,
         * but more queries will be sent to cover the same time period.
         * @param queryTimeWindowSizeMs the size of the query time window in milliseconds
         * @return this builder
         */
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

        public Builder withDefaultPort(int defaultPort) {
            cqlConfigurationBuilder.withDefaultPort(defaultPort);
            return this;
        }

        public Builder withCredentials(String user, String password) {
            cqlConfigurationBuilder.withCredentials(user, password);
            return this;
        }

        public Builder withQueryOptionsFetchSize(int fetchSize) {
            cqlConfigurationBuilder.withQueryOptionsFetchSize(fetchSize);
            return this;
        }

        public Builder withSleepBeforeFirstGenerationMs(long sleepBeforeFirstGenerationMs) {
            masterConfigurationBuilder.withSleepBeforeFirstGenerationMs(sleepBeforeFirstGenerationMs);
            return this;
        }

        public Builder withSleepBeforeGenerationDoneMs(long sleepBeforeGenerationDoneMs) {
            masterConfigurationBuilder.withSleepBeforeGenerationDoneMs(sleepBeforeGenerationDoneMs);
            return this;
        }

        public Builder withSleepAfterExceptionMs(long sleepAfterExceptionMs) {
            masterConfigurationBuilder.withSleepAfterExceptionMs(sleepAfterExceptionMs);
            return this;
        }

        /**
         * Sets the minimal wait time for a worker to wait for a new window (adds latency to each query).
         * @param minimalWaitForWindowMs the minimal wait time in milliseconds
         * @return this builder
         */
        public Builder withMinimalWaitForWindowMs(long minimalWaitForWindowMs) {
            workerConfigurationBuilder.withMinimalWaitForWindowMs(minimalWaitForWindowMs);
            return this;
        }

        /**
         * Sets the <code>Clock</code> in the worker configuration.
         * <p>
         * This clock will be used by the worker to get the
         * current time, for example to determine if it is
         * safe to read the next window.
         * <p>
         * By default a system clock with default time-zone
         * is used.
         *
         * @param clock the clock to set.
         * @return reference to this builder.
         */
        public Builder withClock(Clock clock) {
            workerConfigurationBuilder.withClock(clock);
            return this;
        }

        public Builder withWorkerCQLProvider(Function<Driver3Session, WorkerCQL> workerCQLProvider) {
            this.workerCQLProvider = Preconditions.checkNotNull(workerCQLProvider);
            return this;
        }

        public CDCConsumer build() {
            return new CDCConsumer(cqlConfigurationBuilder.build(),
                    masterConfigurationBuilder, workerConfigurationBuilder, executorServiceSupplier, workerCQLProvider);
        }

        private static int getDefaultWorkersCount() {
            int result = Runtime.getRuntime().availableProcessors() - 1;
            return result > 0 ? result : 1;
        }

        private static Supplier<ScheduledExecutorService> getDefaultExecutorServiceSupplier(int workersCount) {
            return () -> {
                final ScheduledThreadPoolExecutor executor =
                        new ScheduledThreadPoolExecutor(workersCount);
                executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
                return executor;
            };
        }
    }
}
