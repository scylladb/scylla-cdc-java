package com.scylladb.cdc.connector.utils;

import com.scylladb.cdc.connector.alerting.SlackMessage;
import com.google.common.base.Throwables;
import com.scylladb.cdc.model.worker.ScyllaApplicationContext;
import com.scylladb.cdc.model.worker.ScyllaConnectorConfiguration;
import com.scylladb.cdc.model.worker.TableConfig;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.Task;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.util.*;

import static com.scylladb.cdc.connector.utils.JsonUtils.OBJECT_MAPPER;

@Slf4j
public class ScyllaUtils {

    private ScyllaUtils(){ throw new IllegalStateException("Utility Access"); }

    private static ScyllaConnectorConfiguration scyllaConnectorConfigurationObject;

    public static Set<TableName> generateAllTableNames(ScyllaConnectorConfiguration scyllaConnectorConfiguration) {
        scyllaConnectorConfigurationObject = scyllaConnectorConfiguration;
        HashMap<String, List<TableConfig>> keySpacesAndTablesList = scyllaConnectorConfiguration.getKeySpacesAndTablesList();
        Set<TableName> allTableNames = new HashSet<>();
        for (Map.Entry<String, List<TableConfig>> entry : keySpacesAndTablesList.entrySet()) {
            String keySpaceName = entry.getKey();
            List<TableConfig> tablesWithPrimaryKey = entry.getValue();
            for (TableConfig tableAndKey : tablesWithPrimaryKey) {
                String table = tableAndKey.getTableName();
                TableName tableName = new TableName(keySpaceName, table);
                allTableNames.add(tableName);
            }

        }
        log.info("Starting running the scylla connector for topics :");
        printAllTopicNames(allTableNames);
        return allTableNames;
    }

    public static Map<String, String> generateTableToKeyMapping(
        ScyllaConnectorConfiguration scyllaConnectorConfiguration) {
        Map<String, String> tableToPrimaryKeyMap = new HashMap<>();
        HashMap<String, List<TableConfig>> keySpacesAndTablesWithPrimaryKey = scyllaConnectorConfiguration.getKeySpacesAndTablesList();
        for (Map.Entry<String, List<TableConfig>> entry : keySpacesAndTablesWithPrimaryKey.entrySet()) {
            List<TableConfig> tablesWithPrimaryKey = entry.getValue();
            for (TableConfig tableAndKey : tablesWithPrimaryKey) {
                String table = tableAndKey.getTableName();
                String primaryKey = tableAndKey.getPrimaryKey();
                tableToPrimaryKeyMap.put(table, primaryKey);
            }
        }
        return tableToPrimaryKeyMap;

    }

    public static String decodedValue(String fieldName, String fieldValue) {
        int leftSpace = 53 - fieldName.length();

        if (fieldValue.length() > leftSpace) {
            fieldValue = fieldValue.substring(0, leftSpace - 3) + "...";
        }
        return String.format("%1$" + leftSpace + "s", fieldValue);
    }

    public static boolean isSupportedColumnSchema(ChangeSchema.ColumnDefinition cdef) {
        ChangeSchema.CqlType type = cdef.getCdcLogDataType().getCqlType();
        return type != ChangeSchema.CqlType.LIST && type != ChangeSchema.CqlType.MAP &&
                type != ChangeSchema.CqlType.SET && type != ChangeSchema.CqlType.UDT &&
                type != ChangeSchema.CqlType.TUPLE;
    }
    private static void printAllTopicNames(Set<TableName> allTableNames) {
        String orgName = scyllaConnectorConfigurationObject.getOrg();
        String tenantName = scyllaConnectorConfigurationObject.getTenant();

        for (TableName tableName : allTableNames) {
            log.info(orgName + "." + tenantName  + "." + tableName.keyspace + "." + tableName.name);
        }
    }

    public static void doCheckPointing(Task task, RawChange rawChange){
        log.info("Starting checkpointing: " + rawChange.getId().getChangeTime().getTimestamp());
        TableName tableName = task.id.getTable();
        String keySpaceName = tableName.keyspace;     //Note: instance name is same as the keyspace name.
        ChangeId changeId = rawChange.getId();
        long changeTime = changeId.getChangeTime().getTimestamp()/1000;
        ScyllaApplicationContext.updateCheckPoint(keySpaceName,changeTime);
    }

    public static void sendSlackMessage(String slackWebhookUrl, String message){
        try(CloseableHttpClient client = HttpClients.createDefault();) {
            SlackMessage slackMessage = SlackMessage.builder()
                    .channel(ScyllaConstants.SLACK_CHANNEL)
                    .userName(ScyllaConstants.SLACK_USER)
                    .text(message)
                    .iconEmoji(ScyllaConstants.SLACK_EMOJI)
                    .build();
            HttpPost httpPost = new HttpPost(slackWebhookUrl);
            String json = OBJECT_MAPPER.writeValueAsString(slackMessage);
            StringEntity entity = new StringEntity(json);
            httpPost.setEntity(entity);
            httpPost.setHeader("Accept", ScyllaConstants.SLACK_API_TYPE);
            httpPost.setHeader("Content-type", ScyllaConstants.SLACK_API_TYPE);
            client.execute(httpPost);
        } catch (IOException e) {
            log.error("Failed to push slack notification : {}", Throwables.getStackTraceAsString(e.fillInStackTrace()));
        }
    }
}
