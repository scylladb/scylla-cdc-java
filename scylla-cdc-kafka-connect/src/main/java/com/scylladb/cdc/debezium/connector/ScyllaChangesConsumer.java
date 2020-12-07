package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.Task;
import com.scylladb.cdc.model.worker.TaskAndRawChangeConsumer;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class ScyllaChangesConsumer implements TaskAndRawChangeConsumer {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final EventDispatcher<CollectionId> dispatcher;
    private final ScyllaOffsetContext offsetContext;
    private final ScyllaSchema schema;
    private final Clock clock;

    public ScyllaChangesConsumer(EventDispatcher<CollectionId> dispatcher, ScyllaOffsetContext offsetContext, ScyllaSchema schema, Clock clock) {
        this.dispatcher = dispatcher;
        this.offsetContext = offsetContext;
        this.schema = schema;
        this.clock = clock;
    }

    @Override
    public CompletableFuture<Task> consume(Task task, RawChange change) {
        TaskStateOffsetContext taskStateOffsetContext = offsetContext.taskStateOffsetContext(task.id);
        try {
            logger.info("Dispatching change: {}", change.getId().toString());
            dispatcher.dispatchDataChangeEvent(new CollectionId(task.id.getTable()),
                    new ScyllaChangeRecordEmitter(change, taskStateOffsetContext, schema, clock));
        } catch (InterruptedException e) {
            logger.error("Exception while dispatching change: {}", change.getId().toString());
            logger.error("Exception details: {}", e.getMessage());
        }
        return CompletableFuture.completedFuture(task);
    }
}
