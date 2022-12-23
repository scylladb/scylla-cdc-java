package com.scylladb.cdc.connector.utils;

import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.ScyllaApplicationContext;
import com.scylladb.cdc.model.worker.Task;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

@Slf4j
public class CheckPointingTask {

    public void insertTimeStampTask(Task task, RawChange change){
        Timer timer = new Timer();
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                log.info("Starting checkpointing: " + change.getId().getChangeTime().getTimestamp());
                TableName tableName = task.id.getTable();
                String keySpaceName = tableName.keyspace;     //Note: instance name is same as the keyspace name.
                ChangeId changeId = change.getId();
                long changeTime = changeId.getChangeTime().getTimestamp()/1000;
                ScyllaApplicationContext.updateCheckPoint(keySpaceName,changeTime);
            }
        };
        Date date = new Date();
        timer.schedule(timerTask,date,10000);
    }

}
