package com.scylladb.cdc.model.worker;

import com.dview.manifest.db.mysql.config.HikariCpConfig;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.HashMap;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author buntykumar
 * @version 1.0
 */

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ScyllaConnectorConfiguration {
    private String org;
    private String tenant;
    private ScyllaDBConfig scyllaDBConfig;
    private String sourceIdOrName;
    private HashMap<String, List<TableConfig>> keySpacesAndTablesList;
    private Integer workersCount;
    private int batchSize;
    private String kafkaBrokers;
    private int poolSize;
    private String slackWebhookURL;
    private HikariCpConfig dataSourceConfig;
    private int checkPointAfterRows;
}
