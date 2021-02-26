package com.scylladb.cdc.replicator.operations.postimage;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.replicator.driver3.Driver3FromLibraryTranslator;
import com.scylladb.cdc.replicator.operations.CdcOperationHandler;
import com.scylladb.cdc.replicator.operations.insert.InsertOperationHandler;
import com.scylladb.cdc.replicator.operations.update.PreparedUpdateOperationHandler;

import java.util.concurrent.ConcurrentHashMap;

public class PostImageState {
    private final ConcurrentHashMap<StreamId, CdcOperationHandler> lastOperationHandler = new ConcurrentHashMap<>();
    private final InsertOperationHandler insertOperationHandler;
    private final PreparedUpdateOperationHandler updateOperationHandler;

    public PostImageState(Session session, Driver3FromLibraryTranslator d3t, TableMetadata table) {
        this.insertOperationHandler = new InsertOperationHandler(session, d3t, table);
        this.updateOperationHandler = new PreparedUpdateOperationHandler(session, d3t, table);
    }

    public void addInsertOperation(RawChange change) {
        lastOperationHandler.put(change.getId().getStreamId(), insertOperationHandler);
    }

    public void addUpdateOperation(RawChange change) {
        lastOperationHandler.put(change.getId().getStreamId(), updateOperationHandler);
    }

    public CdcOperationHandler getLastOperationHandler(RawChange change) {
        return lastOperationHandler.get(change.getId().getStreamId());
    }
}
