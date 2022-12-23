package com.scylladb.cdc.connector.connector;

import com.scylladb.cdc.connector.transform.ITransformer;
import com.scylladb.cdc.connector.transform.ScyllaTransformer;
import com.scylladb.cdc.connector.utils.ScyllaUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.scylladb.cdc.model.worker.CheckPointDetails;
import com.scylladb.cdc.model.worker.ScyllaApplicationContext;
import com.scylladb.cdc.model.worker.ScyllaConnectorConfiguration;
import com.scylladb.cdc.lib.CDCConsumer;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.*;
import lombok.extern.slf4j.Slf4j;
import sun.misc.Signal;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

/**
 * @author buntykumar
 * @version 1.0
 */

@Slf4j
public class ScyllaConnectorTask{

    private final ScyllaConnectorConfiguration scyllaConnectorConfiguration;
    private final ITransformer iTransformer;
    private boolean continuousFlag;

    public ScyllaConnectorTask(ScyllaConnectorConfiguration scyllaConnectorConfiguration) {
        this.scyllaConnectorConfiguration = scyllaConnectorConfiguration;
        this.iTransformer = new ScyllaTransformer(scyllaConnectorConfiguration);

    }

    /**
     * This code block will read log table, transform and push
     * the event into kafka.
     */

    public void startReplication(boolean restart,String instanceName) throws InterruptedException {
        long lastAccessedTimeStamp = 0;
        if(restart){
            log.info("Getting the timestamp processed a minute ago: ");
            CheckPointDetails lastAccessedDetails = ScyllaApplicationContext.getCheckPointDetails(instanceName);
            lastAccessedTimeStamp = lastAccessedDetails.getLastReadTimestamp();
        }

        long finalLastAccessedTimeStamp = lastAccessedTimeStamp;
        TaskAndRawChangeConsumer changeConsumer = (task, change) -> {
            try {
                if (!restart || continuousFlag) {
                    processChanges(task, change);
                }else {
                    log.info("Waiting for the changed which was last accessed: ");
                    ChangeId changeId = change.getId();
                    ChangeTime changeTime = changeId.getChangeTime();
                    long timeInMillis = changeTime.getTimestamp();
                    if(finalLastAccessedTimeStamp == timeInMillis){
                        processChanges(task, change);
                        continuousFlag = true;
                    }
                }
            } catch (JsonProcessingException | ExecutionException e) {
                log.error("Exception while processing the change: " + e.getMessage());
            }
            return CompletableFuture.completedFuture(null);
        };

        Set<TableName> tableNames = ScyllaUtils.generateAllTableNames(scyllaConnectorConfiguration);
        List<InetSocketAddress> inetSocketAddresses = new ArrayList<>();
        new ArrayList<>(Arrays.asList(scyllaConnectorConfiguration.getScyllaDBConfig().getHost().split(","))).forEach(inet -> inetSocketAddresses.add(new InetSocketAddress(inet, scyllaConnectorConfiguration.getScyllaDBConfig().getPort())));

        CDCConsumer consumer = CDCConsumer.builder()
                .addContactPoints(inetSocketAddresses)
                .addTables(tableNames)
                .withWorkersCount(scyllaConnectorConfiguration.getWorkersCount())
                .withTaskAndRawChangeConsumer(changeConsumer)
                .withCredentials(scyllaConnectorConfiguration.getScyllaDBConfig().getUserName(), scyllaConnectorConfiguration.getScyllaDBConfig().getScPassword())
                .build();

        consumer.start();
        CountDownLatch terminationLatch = new CountDownLatch(1);
        Signal.handle(new Signal("INT"), signal -> terminationLatch.countDown());
        terminationLatch.await();
        consumer.stop();
    }

    public void processChanges(Task task, RawChange change) throws JsonProcessingException, ExecutionException {
        try {
            iTransformer.transformAndPush(task, change);
        } catch (Exception e) {
            ScyllaUtils.sendSlackMessage(scyllaConnectorConfiguration.getSlackWebhookURL(), e.getMessage());
        }


        /**
         * The below code can be enabled if we want to push all the post changes as insert.
         * iTransformer.transformAndPushOnlyPOST(task,change);
         */
    }
}
