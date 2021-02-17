package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.Bytes;
import com.scylladb.cdc.cql.driver3.Driver3WorkerCQL;
import com.scylladb.cdc.model.ExponentialRetryBackoffWithJitter;
import com.scylladb.cdc.model.RetryBackoff;
import com.scylladb.cdc.model.worker.Connectors;
import com.scylladb.cdc.model.worker.Worker;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.util.Clock;
import org.apache.commons.lang3.tuple.Pair;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ScyllaStreamingChangeEventSource implements StreamingChangeEventSource {
    private static final RetryBackoff DEFAULT_WORKER_RETRY_BACKOFF =
            new ExponentialRetryBackoffWithJitter(10, 30000);

    private final ScyllaConnectorConfig configuration;
    private ScyllaTaskContext taskContext;
    private final ScyllaOffsetContext offsetContext;
    private final ScyllaSchema schema;
    private final EventDispatcher<CollectionId> dispatcher;
    private final Clock clock;
    private final Duration pollInterval;

    public ScyllaStreamingChangeEventSource(ScyllaConnectorConfig configuration, ScyllaTaskContext taskContext, ScyllaOffsetContext offsetContext, ScyllaSchema schema, EventDispatcher<CollectionId> dispatcher, Clock clock) {
        this.configuration = configuration;
        this.taskContext = taskContext;
        this.offsetContext = offsetContext;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.pollInterval = configuration.getPollInterval();
    }

    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        Cluster cluster = new ScyllaClusterBuilder(configuration).build();
        Session session = cluster.connect();
        Driver3WorkerCQL cql = new Driver3WorkerCQL(session);
        ScyllaWorkerTransport workerTransport = new ScyllaWorkerTransport(context, offsetContext, dispatcher);
        ScyllaChangesConsumer changeConsumer = new ScyllaChangesConsumer(dispatcher, offsetContext, schema, clock);
        Worker worker = new Worker(new Connectors(workerTransport, cql, changeConsumer, configuration.getQueryTimeWindowSizeMs(),
                configuration.getConfidenceWindowSizeMs(), DEFAULT_WORKER_RETRY_BACKOFF));

        try {
            worker.run(taskContext.getTasks().stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
        } catch (ExecutionException e) {
            // TODO - throw user-friendly error and do proper validation
        }
    }
}
