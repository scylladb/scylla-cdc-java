package com.scylladb.cdc.connector.connector;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.scylladb.cdc.connector.core.IScyllaConnector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.scylladb.cdc.model.worker.ScyllaApplicationContext;
import com.scylladb.cdc.model.worker.ScyllaConnectorConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * @author buntykumar
 * @version 1.0
 */

@Slf4j
public class ScyllaConnector implements IScyllaConnector {

    @Parameter(names = "--file", required = true, description = "Config file for the scylla connector: ")
    private String configFilePath;

    private ScyllaConnectorTask scyllaConnectorTask;

    @Override
    public void initialize() throws IOException {
        log.info("Initializing the configuration for the scylla connector: " + configFilePath);
        ScyllaConnectorConfiguration scyllaConnectorConfiguration = new ObjectMapper(new YAMLFactory())
            .readValue(new File(configFilePath), ScyllaConnectorConfiguration.class);
        log.debug(ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE));
        ScyllaApplicationContext.setScyllaConfiguration(scyllaConnectorConfiguration);
        ScyllaApplicationContext.setInstanceName(scyllaConnectorConfiguration.getSourceIdOrName());
        this.scyllaConnectorTask = new ScyllaConnectorTask(scyllaConnectorConfiguration);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ScyllaConnector scyllaConnector = new ScyllaConnector();
        scyllaConnector.setShutDownHook();
        JCommander.newBuilder().addObject(scyllaConnector).build().parse(args);
        scyllaConnector.initialize();
        scyllaConnector.start();
    }


    @Override
    public void start() throws InterruptedException {
        log.info("-------Starting scylla-cdc-connector--------");
        scyllaConnectorTask.startReplication();
    }

    /**
     * Registers a new virtual-machine shutdown hook.
     *
     * <p> The Java virtual machine <i>shuts down</i> in response to two kinds
     * of events:
     * <ul>
     * <li> The program <i>exits</i> normally, when the last non-daemon
     * thread exits or when the <tt>{@link Runtime#exit}</tt> (equivalently,
     * {@link System#exit(int) System.exit}) method is invoked, or
     * <li> The virtual machine is <i>terminated</i> in response to a
     * user interrupt, such as typing <tt>^C</tt>, or a system-wide event,
     * such as user logoff or system shutdown.
     * </ul>
     * </p>
     * <p>
     * For further more information on shutdown hook {@link Runtime #addShutdownHook}
     * </p>
     */
    public void setShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread("Fiber-SCYLLADB-shutdown-hook") {
            @Override
            public void run() {
                log.warn("Stopping Fiber-SCYLLA-DB commuting process!!!");
                Set<Thread> runningThreads = Thread.getAllStackTraces().keySet();
                for (Thread th : runningThreads) {
                    if (th != Thread.currentThread() && !th.isDaemon() && th.getClass().getName().startsWith("com.scylladb.cdc.connector.connector")) {
                        log.warn("Interrupting the {} for termination", th.getClass());
                        th.interrupt();
                    } else if (th != Thread.currentThread() && !th.isDaemon() && th.isInterrupted()) {
                        log.warn("Waiting {} for termination", th.getName());
                        try {
                            th.join();
                        } catch (Exception e) {
                            log.error("Failed due to {}", e.getMessage());
                        }
                    }
                }
                log.warn("<------- Shutdown Hook Called for Fiber-SCYLLA-DB Instance ---------------->");
            }
        });
    }
}
