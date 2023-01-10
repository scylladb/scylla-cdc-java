package com.scylladb.cdc.connector.transform;

import com.scylladb.cdc.connector.cache.UtilityCache;
import com.scylladb.cdc.connector.kafka.KafkaConnector;
import com.scylladb.cdc.connector.utils.JsonUtils;
import com.scylladb.cdc.connector.utils.ScyllaConstants;
import com.scylladb.cdc.connector.utils.ScyllaUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.scylladb.cdc.cql.driver3.Driver3Field;
import com.scylladb.cdc.model.worker.ScyllaConnectorConfiguration;
import com.scylladb.cdc.model.worker.*;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.model.worker.cql.CqlDate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;


@Slf4j
public class ScyllaTransformer implements ITransformer {

    private final ScyllaConnectorConfiguration scyllaConnectorConfiguration;

    private final KafkaConnector kafkaConnector;

    private Map<String, String> tableToPrimaryKeyMap = new HashMap<>();

    private final SimpleDateFormat formatter = new SimpleDateFormat(ScyllaConstants.DATETIME_FORMAT);

    private final Map<String, Long> checkpointMap = Collections.synchronizedMap(new HashMap<>());

    public ScyllaTransformer(ScyllaConnectorConfiguration scyllaConnectorConfiguration) {
        this.kafkaConnector = new KafkaConnector(scyllaConnectorConfiguration);
        this.scyllaConnectorConfiguration = scyllaConnectorConfiguration;
    }

    /**
     * This method is the crux of this application which is responsible to get the task and change(row change) parameter
     * and build a json payload which will have some metadata also like createdAt and updatedAt
     */
    private void processCheckpoint(String uniqueIdentifier){
        //Case of polling from long history and data surge is High
        ScyllaApplicationContext.updateCheckPoint(uniqueIdentifier, checkpointMap.get(uniqueIdentifier));
        log.info("Pushed record successfully for: {}", uniqueIdentifier);
    }

    @Override
    public void transformAndPush(Task task, RawChange change) throws JsonProcessingException, ExecutionException {
        getTableToPrimaryKeyMap();

        //Fetching write time of the record.
        ChangeId changeId = change.getId();
        ChangeTime changeTime = changeId.getChangeTime();
        long cddTimeStamp = changeTime.getTimestamp();
        String topicName = topicNameBuilder(task);

        Map<String, Object> payloadMap = buildPayload(task, change, cddTimeStamp);
        payloadMap.put(ScyllaConstants.SCHEMA_VERSION, "1.0");

        String kafkaPayload = JsonUtils.OBJECT_MAPPER.writeValueAsString(payloadMap);


        if (RawChange.OperationType.ROW_INSERT.equals(change.getOperationType()) || (RawChange.OperationType.POST_IMAGE.equals(change.getOperationType()))) {
            if (payloadMap.size() > 2) {
                kafkaConnector.getConnector().send(new ProducerRecord<>(topicName, payloadMap.get(ScyllaConstants.ENTITY_ID).toString(), kafkaPayload));
                String uniqueIdentifier = String.format("%s$%s", task.id.getTable().keyspace, task.id.getTable().name);
                System.out.println(kafkaPayload);
                //Process Checkpointing
                long oldCheckpoint;
                if (Objects.isNull(checkpointMap.get(uniqueIdentifier))) {
                    oldCheckpoint = ScyllaApplicationContext.getCheckPointDetails(uniqueIdentifier).getLastReadTimestamp();
                    checkpointMap.put(uniqueIdentifier, oldCheckpoint);
                } else {
                    oldCheckpoint = checkpointMap.get(uniqueIdentifier);
                }

                //Current Checkpoint
                long cdcTimeStamp = cddTimeStamp/1000;

                //Trigger point for checkpointing
                if(cdcTimeStamp > oldCheckpoint && cdcTimeStamp - oldCheckpoint >= (6 * WorkerConfiguration.DEFAULT_QUERY_TIME_WINDOW_SIZE_MS)){
                    checkpointMap.put(uniqueIdentifier, cddTimeStamp/1000);
                    processCheckpoint(uniqueIdentifier);
                }
            }
        }
    }



    /**
     * This is a utility method to build the topic where a particular change
     * will get pushed.
     */
    private String topicNameBuilder(Task task) {
        String org = scyllaConnectorConfiguration.getOrg();
        String tenant = scyllaConnectorConfiguration.getTenant();
        String keyspaceName = task.id.getTable().keyspace;
        String tableName = task.id.getTable().name;
        return String.format("%s.%s.%s.%s", org, tenant, keyspaceName, tableName);
    }

    /**
     * This method will iterate the change and get the actual data and put it to the payload map.
     *
     * @param payload is partially built with the metadata columns.
     */
    private void putDataColumns(Map<String, Object> payload, RawChange change) {
        for (ChangeSchema.ColumnDefinition columnDefinition : change.getSchema().getNonCdcColumnDefinitions()) {
            //if (!ScyllaUtils.isSupportedColumnSchema(columnDefinition)) continue;
            Cell columnName = change.getCell(columnDefinition.getColumnName());
            Object value = translateCellToKafka(columnName);
            payload.put(columnDefinition.getColumnName(), value);
        }
    }

    private Object translateCellToKafka(Cell cell) {
        ChangeSchema.DataType dataType = cell.getColumnDefinition().getCdcLogDataType();

        if (cell.getAsObject() == null) {
            return null;
        }

        if (dataType.getCqlType() == ChangeSchema.CqlType.DECIMAL) {
            return cell.getDecimal().toString();
        }

        if (dataType.getCqlType() == ChangeSchema.CqlType.UUID) {
            return cell.getUUID().toString();
        }

        if (dataType.getCqlType() == ChangeSchema.CqlType.TIMEUUID) {
            return cell.getUUID().toString();
        }

        if (dataType.getCqlType() == ChangeSchema.CqlType.UDT) {
            return convertIntoJavaObject(cell);
        }

        if (dataType.getCqlType() == ChangeSchema.CqlType.VARINT) {
            return cell.getVarint().toString();
        }

        if (dataType.getCqlType() == ChangeSchema.CqlType.INET) {
            return cell.getInet().getHostAddress();
        }

        if (dataType.getCqlType() == ChangeSchema.CqlType.TIMESTAMP) {
            Date date = cell.getTimestamp();
            return formatter.format(date);
        }

        if (dataType.getCqlType() == ChangeSchema.CqlType.DATE) {
            CqlDate cqlDate = cell.getDate();
            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            calendar.clear();
            // Months start from 0 in Calendar:
            calendar.set(cqlDate.getYear(), cqlDate.getMonth() - 1, cqlDate.getDay());
            return Date.from(calendar.toInstant());
        }

        if (dataType.getCqlType() == ChangeSchema.CqlType.DURATION) {
            return cell.getDuration().toString();
        }

        return cell.getAsObject();
    }

    /**
     * This method is responsible to build the payload from scratch.
     * It has the logic of handling the insert,update and delete appropriately
     */
    private Map<String, Object> buildPayload(Task task, RawChange change, long timestamp) throws ExecutionException {
        String namespace = task.id.getTable().keyspace;
        String objectName = task.id.getTable().name;
        String org = scyllaConnectorConfiguration.getOrg();
        String tenant = scyllaConnectorConfiguration.getTenant();

        //Taking the operation type:
        RawChange.OperationType operationType = change.getOperationType();
        String opTypeString = operationType.toString();

        Map<String, Object> transformPayloadMap = Collections.synchronizedMap(new TreeMap<>());
        putMetaColumns(transformPayloadMap, org, tenant, namespace, objectName, timestamp);

        switch (opTypeString) {
            case ScyllaConstants.ROW_INSERT:
                transformPayloadMap.put(ScyllaConstants.OPCODE, ScyllaConstants.INSERT_OPCODE);
                putDataColumns(transformPayloadMap, change);
                UtilityCache.put(ScyllaConstants.UPDATED_CACHE, false);
                break;
            case ScyllaConstants.ROW_UPDATE:
                UtilityCache.put(ScyllaConstants.UPDATED_CACHE, true);
                break;
            case ScyllaConstants.ROW_DELETE:
                UtilityCache.put("delete", true);
                break;
            case ScyllaConstants.POST_IMAGE:
                boolean isUpdate = UtilityCache.get(ScyllaConstants.UPDATED_CACHE);
                if (isUpdate) {
                    transformPayloadMap.put(ScyllaConstants.OPCODE, ScyllaConstants.UPDATE_OPCODE);
                    putDataColumns(transformPayloadMap, change);
                    UtilityCache.put(ScyllaConstants.UPDATED_CACHE, false);
                    break;
                } else {
                    transformPayloadMap.clear();
                }
                break;
            default:
                throw new IllegalStateException(String.format("WARN: OPCODE: %s, Namespace: %s & ObjectName: %s", opTypeString, namespace, objectName));
        }

        String primaryKey = tableToPrimaryKeyMap.get(objectName);
        Object primaryKeyValue = change.getCell(primaryKey).getAsObject();
        transformPayloadMap.put(ScyllaConstants.ENTITY_ID, primaryKeyValue);

        return transformPayloadMap;
    }

    public void putMetaColumns(Map<String, Object> transformPayloadMap, String org, String tenant, String namespace, String objectName, long timestamp) {
        transformPayloadMap.put(ScyllaConstants.CREATED_AT, timestamp);
        transformPayloadMap.put(ScyllaConstants.UPDATED_AT, timestamp);
        transformPayloadMap.put(ScyllaConstants.INGESTED_AT, timestamp);
        transformPayloadMap.put(ScyllaConstants.ORG, org);
        transformPayloadMap.put(ScyllaConstants.NAMESPACE, namespace);
        transformPayloadMap.put(ScyllaConstants.OBJECT_NAME, objectName);
        transformPayloadMap.put(ScyllaConstants.TENANT, tenant);
    }

    @Override
    public void transformAndPushOnlyPOST(Task task, RawChange change) throws JsonProcessingException {
        getTableToPrimaryKeyMap();

        ChangeId changeId = change.getId();
        ChangeTime changeTime = changeId.getChangeTime();
        long timeInMillis = changeTime.getTimestamp();

        String topicName = topicNameBuilder(task);
        RawChange.OperationType operationType = change.getOperationType();
        String opTypeString = operationType.toString();

        if(opTypeString.equals("POST_IMAGE")){
            Map<String, Object> payloadMap = buildPayloadPostImage(task, change, timeInMillis);
            payloadMap.put(ScyllaConstants.SCHEMA_VERSION, "1.0");

            String kafkaPayload = JsonUtils.OBJECT_MAPPER.writeValueAsString(payloadMap);
            kafkaConnector.getConnector().send(new ProducerRecord<>(topicName, payloadMap.get(ScyllaConstants.ENTITY_ID).toString(), kafkaPayload));
        }
    }

    public Map<String, Object> buildPayloadPostImage(Task task, RawChange change, long timestamp) {
        String namespace = task.id.getTable().keyspace;
        String objectName = task.id.getTable().name;
        String org = scyllaConnectorConfiguration.getOrg();
        String tenant = scyllaConnectorConfiguration.getTenant();

        //Taking the operation type:
        RawChange.OperationType operationType = change.getOperationType();
        String opTypeString = operationType.toString();

        Map<String, Object> transformPayloadMapPostImage = Collections.synchronizedMap(new TreeMap<>());
        putMetaColumns(transformPayloadMapPostImage, org, tenant, namespace, objectName, timestamp);

        if (opTypeString.equals("POST_IMAGE")) {
            putDataColumns(transformPayloadMapPostImage, change);
            String primaryKey = tableToPrimaryKeyMap.get(objectName);
            Object primaryKeyValue = change.getCell(primaryKey).getAsObject();
            transformPayloadMapPostImage.put(ScyllaConstants.ENTITY_ID, primaryKeyValue);
            transformPayloadMapPostImage.put(ScyllaConstants.OPCODE, ScyllaConstants.INSERT_OPCODE);
        }
        return transformPayloadMapPostImage;
    }

    private void getTableToPrimaryKeyMap() {
        tableToPrimaryKeyMap = ScyllaUtils.generateTableToKeyMapping(scyllaConnectorConfiguration);
    }

    private LinkedHashMap<String,Object> convertIntoJavaObject(Cell cell){
        LinkedHashMap<String,Object> convertedMap = new LinkedHashMap<>();
        Object value = cell.getAsObject();
        LinkedHashMap<String, Driver3Field> map = (LinkedHashMap<String, Driver3Field>) value;   //Using this approach as the object mapper approach is throwing error

        for (Map.Entry<String, Driver3Field> entry : map.entrySet()) {
            ChangeSchema.DataType dataType = entry.getValue().getDataType();
            String key = entry.getKey();
            String valueDataType = dataType.getCqlType().name();
            switch (valueDataType){
                case "BIGINT":
                    convertedMap.put(key,entry.getValue().getLong());
                    break;
                case "TIMESTAMP":
                    Date date = cell.getTimestamp();
                    convertedMap.put(key,formatter.format(date));
                    break;
                default:
                    convertedMap.put(key,entry.getValue().toString());
            }
        }
        return convertedMap;
    }
}
