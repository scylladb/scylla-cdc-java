package com.scylladb.cdc.replicator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.cql.driver3.Driver3FromLibraryTranslator;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import com.scylladb.cdc.replicator.operations.CdcOperationHandler;
import com.scylladb.cdc.replicator.operations.NoOpOperationHandler;
import com.scylladb.cdc.replicator.operations.delete.PartitionDeleteOperationHandler;
import com.scylladb.cdc.replicator.operations.delete.RangeDeleteEndOperationHandler;
import com.scylladb.cdc.replicator.operations.delete.RangeDeleteStartOperationHandler;
import com.scylladb.cdc.replicator.operations.delete.RangeDeleteState;
import com.scylladb.cdc.replicator.operations.delete.RowDeleteOperationHandler;
import com.scylladb.cdc.replicator.operations.insert.InsertOperationHandler;
import com.scylladb.cdc.replicator.operations.postimage.PostImageInsertUpdateOperationHandler;
import com.scylladb.cdc.replicator.operations.postimage.PostImageOperationHandler;
import com.scylladb.cdc.replicator.operations.postimage.PostImageState;
import com.scylladb.cdc.replicator.operations.preimage.PreImageOperationHandler;
import com.scylladb.cdc.replicator.operations.update.PreparedUpdateOperationHandler;
import com.scylladb.cdc.replicator.operations.update.UnpreparedUpdateOperationHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ReplicatorConsumer implements RawChangeConsumer {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final ConsistencyLevel consistencyLevel;
    private final Map<RawChange.OperationType, CdcOperationHandler> operationHandlers = new HashMap<>();

    public ReplicatorConsumer(Main.Mode replicatorMode, Cluster destinationCluster, Session destinationSession,
                              String keyspace, String tableName, ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;

        TableMetadata sourceTableMetadata = destinationCluster.getMetadata().getKeyspace(keyspace).getTable(tableName);

        // Setup a helper object, which translates changes returned by the library
        // to the Scylla Java Driver version 3 representation. This translated
        // representation then can be used directly with the driver.
        Driver3FromLibraryTranslator driver3FromLibraryTranslator = new Driver3FromLibraryTranslator(destinationCluster.getMetadata());

        // The ReplicatorConsumer will consume RawChanges from the CDC log.
        // For each change it executes a handler based on its operation type.
        // Most of the handlers read the change and execute it on a destination
        // cluster (subclasses of ExecutingStatementHandler) - replicating the change.
        operationHandlers.put(RawChange.OperationType.PRE_IMAGE, new NoOpOperationHandler());
        operationHandlers.put(RawChange.OperationType.ROW_UPDATE, new PreparedUpdateOperationHandler(destinationSession, driver3FromLibraryTranslator, sourceTableMetadata));
        operationHandlers.put(RawChange.OperationType.ROW_INSERT, new InsertOperationHandler(destinationSession, driver3FromLibraryTranslator, sourceTableMetadata));
        operationHandlers.put(RawChange.OperationType.ROW_DELETE, new RowDeleteOperationHandler(destinationSession, driver3FromLibraryTranslator, sourceTableMetadata));
        operationHandlers.put(RawChange.OperationType.PARTITION_DELETE, new PartitionDeleteOperationHandler(destinationSession, driver3FromLibraryTranslator, sourceTableMetadata));

        // Row range deletes in Scylla CDC log are represented as two rows: left and right bound.
        // RangeDeleteState stores the left bound to be used when combined with right bound.
        RangeDeleteState rangeDeleteState = new RangeDeleteState();
        operationHandlers.put(RawChange.OperationType.ROW_RANGE_DELETE_INCLUSIVE_LEFT_BOUND,
                new RangeDeleteStartOperationHandler(rangeDeleteState, true));
        operationHandlers.put(RawChange.OperationType.ROW_RANGE_DELETE_EXCLUSIVE_LEFT_BOUND,
                new RangeDeleteStartOperationHandler(rangeDeleteState, false));
        operationHandlers.put(RawChange.OperationType.ROW_RANGE_DELETE_INCLUSIVE_RIGHT_BOUND, new RangeDeleteEndOperationHandler(destinationSession,
                sourceTableMetadata, driver3FromLibraryTranslator, rangeDeleteState, true));
        operationHandlers.put(RawChange.OperationType.ROW_RANGE_DELETE_EXCLUSIVE_RIGHT_BOUND, new RangeDeleteEndOperationHandler(destinationSession,
                sourceTableMetadata, driver3FromLibraryTranslator, rangeDeleteState, false));

        operationHandlers.put(RawChange.OperationType.POST_IMAGE, new NoOpOperationHandler());

        // If the replicated table has a non-frozen collection, for example (list<int>):
        //
        // CREATE TABLE ks.t(pk int, ck int, v list<int>, PRIMARY KEY(pk, ck)) WITH cdc = {'enabled': true};
        //
        // there could be an operation like this:
        //
        // UPDATE ks.t SET v = v + [1, 2] WHERE pk = 0 AND ck = 0;
        //
        // that cannot be handled by a prepared statement: CDC log contains only the added elements,
        // not the final result of the operation.
        //
        // Therefore, in such case do not use prepared statements.
        boolean hasNonFrozenCollection = sourceTableMetadata.getColumns().stream().anyMatch(c -> c.getType().isCollection() && !c.getType().isFrozen());
        if (hasNonFrozenCollection) {
            operationHandlers.put(RawChange.OperationType.ROW_UPDATE, new UnpreparedUpdateOperationHandler(destinationSession, sourceTableMetadata, driver3FromLibraryTranslator));
        }

        // If the replicator runs in a PRE_IMAGE mode, when it encounters
        // a PRE_IMAGE row, it checks the destination cluster
        // to make sure the row contains the same data as in PRE_IMAGE.
        if (replicatorMode == Main.Mode.PRE_IMAGE) {
            operationHandlers.put(RawChange.OperationType.PRE_IMAGE, new PreImageOperationHandler(destinationSession, driver3FromLibraryTranslator, sourceTableMetadata));
        }

        // If the replicator runs in POST_IMAGE mode, when it encounters
        // an INSERT or UPDATE operation (which might only modify a
        // part of the row), it waits for POST_IMAGE (which contains
        // the full row) and performs the INSERT/UPDATE with
        // the full data from POST_IMAGE.
        if (replicatorMode == Main.Mode.POST_IMAGE) {
            PostImageState postImageState = new PostImageState(destinationSession, driver3FromLibraryTranslator, sourceTableMetadata);

            operationHandlers.put(RawChange.OperationType.ROW_UPDATE, new PostImageInsertUpdateOperationHandler(postImageState));
            operationHandlers.put(RawChange.OperationType.ROW_INSERT, new PostImageInsertUpdateOperationHandler(postImageState));
            operationHandlers.put(RawChange.OperationType.POST_IMAGE, new PostImageOperationHandler(postImageState));
        }
    }

    @Override
    public CompletableFuture<Void> consume(RawChange change) {
        logger.atInfo().log("Replicator consuming change: %s, %s", change.getId(), change.getOperationType());

        // Get the proper handler for change.
        RawChange.OperationType operationType = change.getOperationType();
        CdcOperationHandler handler = operationHandlers.get(operationType);
        if (handler == null) {
            throw new UnsupportedOperationException(operationType.toString());
        }

        // Execute the handler on change.
        return handler.handle(change, consistencyLevel);
    }
}
