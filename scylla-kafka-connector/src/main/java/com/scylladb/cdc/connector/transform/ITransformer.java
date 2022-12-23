package com.scylladb.cdc.connector.transform;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.Task;

import java.util.concurrent.ExecutionException;

public interface ITransformer {
    void transformAndPush(Task task, RawChange rawChange) throws JsonProcessingException, ExecutionException;

    void transformAndPushOnlyPOST(Task task,RawChange rawChange) throws JsonProcessingException;
}
