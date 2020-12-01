package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.TaskState;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.DataCollectionId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;
import java.util.Map;

public class TaskStateOffsetContext implements OffsetContext {

    private final ScyllaOffsetContext offsetContext;
    private final TaskId taskId;
    private final SourceInfo sourceInfo;

    public TaskStateOffsetContext(ScyllaOffsetContext offsetContext, TaskId taskId, SourceInfo sourceInfo) {
        this.offsetContext = offsetContext;
        this.taskId = taskId;
        this.sourceInfo = sourceInfo;
    }

    @Override
    public Map<String, ?> getPartition() {
        return sourceInfo.partition(taskId);
    }

    @Override
    public Map<String, ?> getOffset() {
        return sourceInfo.offset(taskId);
    }

    public TaskState getTaskState() {
        return sourceInfo.getTaskState(taskId);
    }

    public void dataChangeEvent(TaskState taskState) {
        sourceInfo.dataChangeEvent(taskId, taskState);
    }

    @Override
    public Schema getSourceInfoSchema() {
        return offsetContext.getSourceInfoSchema();
    }

    @Override
    public Struct getSourceInfo() {
        return offsetContext.getSourceInfo();
    }

    @Override
    public boolean isSnapshotRunning() {
        return offsetContext.isSnapshotRunning();
    }

    @Override
    public void markLastSnapshotRecord() {
        offsetContext.markLastSnapshotRecord();
    }

    @Override
    public void preSnapshotStart() {
        offsetContext.preSnapshotStart();
    }

    @Override
    public void preSnapshotCompletion() {
        offsetContext.preSnapshotCompletion();
    }

    @Override
    public void postSnapshotCompletion() {
        offsetContext.postSnapshotCompletion();
    }

    @Override
    public void event(DataCollectionId dataCollectionId, Instant instant) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TransactionContext getTransactionContext() {
        return offsetContext.getTransactionContext();
    }
}
