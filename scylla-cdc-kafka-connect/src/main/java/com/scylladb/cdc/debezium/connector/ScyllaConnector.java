package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.scylladb.cdc.cql.driver3.Driver3MasterCQL;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.master.Master;
import io.debezium.config.Configuration;
import io.debezium.util.Threads;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ScyllaConnector extends SourceConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, String> props;
    private Configuration config;

    private ScyllaMasterTransport masterTransport;
    private ExecutorService masterExecutor;
    private Master master;
    private Future<?> masterFuture;

    public ScyllaConnector() {
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;

        final Configuration config = Configuration.from(props);
        final ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);
        this.config = config;

        // TODO - properly close the session
        List<InetSocketAddress> contactPoints = ConfigSerializerUtil.deserializeClusterIpAddresses(config.getString(ScyllaConnectorConfig.CLUSTER_IP_ADDRESSES));
        List<InetAddress> contactHosts = contactPoints.stream().map(InetSocketAddress::getAddress).collect(Collectors.toList());
        int contactPort = contactPoints.stream().map(InetSocketAddress::getPort).findFirst().get();

        Cluster cluster = Cluster.builder().addContactPoints(contactHosts).withPort(contactPort).build();
        Session session = cluster.connect();
        Driver3MasterCQL cql = new Driver3MasterCQL(session);
        this.masterTransport = new ScyllaMasterTransport(context(), new SourceInfo(connectorConfig));
        Set<TableName> tableNames = ConfigSerializerUtil.deserializeTableNames(config.getString(ScyllaConnectorConfig.TABLE_NAMES));
        this.master = new Master(masterTransport, cql, tableNames);

        this.masterExecutor = Threads.newSingleThreadExecutor(ScyllaConnector.class, config.getString(ScyllaConnectorConfig.LOGICAL_NAME), "scylla-lib-master-executor");
        this.masterFuture = this.masterExecutor.submit(() -> {
            try {
                master.run();
            } catch (ExecutionException e) {
                // TODO - handle exception
            }
        });
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ScyllaConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<TaskId, SortedSet<StreamId>> tasks = masterTransport.getWorkerConfigurations();
        List<String> workerConfigs = new TaskConfigBuilder(tasks).buildTaskConfigs(maxTasks);
        return workerConfigs.stream().map(c -> config.edit()
                .with(ScyllaConnectorConfig.WORKER_CONFIG, c)
                .build().asMap()).collect(Collectors.toList());
    }

    @Override
    public void stop() {
        // TODO - properly close and stop all resources
        this.masterFuture.cancel(true);
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ConfigDef config() {
        return ScyllaConnectorConfig.configDef();
    }
}
