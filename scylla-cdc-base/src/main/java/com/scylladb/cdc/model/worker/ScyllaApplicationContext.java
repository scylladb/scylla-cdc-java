package com.scylladb.cdc.model.worker;

import com.dview.manifest.db.mysql.config.HikariCpConfig;
import com.dview.manifest.db.mysql.helper.IMysqlDaoHelper;
import com.dview.manifest.db.mysql.helper.MysqlDaoHelper;
import com.dview.manifest.db.mysql.transaction.DataSourceFactory;
import com.dview.manifest.metrics.IApplicationMetrics;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import javax.sql.DataSource;
import lombok.Getter;
import lombok.Setter;


public class ScyllaApplicationContext {

  private ScyllaApplicationContext(){ throw new IllegalStateException("Utility Class"); }
  @Getter
  private static ScyllaConnectorConfiguration scyllaConnectorConfiguration;
  private static IMysqlDaoHelper mysqlDaoHelper;

  @Setter
  @Getter
  private static String instanceName;

  public static void setScyllaConfiguration(ScyllaConnectorConfiguration scyllaConfiguration) {
    scyllaConnectorConfiguration = scyllaConfiguration;
  }

  public static IMysqlDaoHelper getMysqlDaoHelper() {
    if (Objects.isNull(mysqlDaoHelper)) {
      DataSource dataSource = HikariCpConfig.getDataSource(ScyllaApplicationContext.scyllaConnectorConfiguration.getDataSourceConfig());
      DataSourceFactory.registerDataSource("manifest-metastore-schema-source", dataSource);
      DataSourceFactory.registerDefaultDataSource(dataSource);
      mysqlDaoHelper = new MysqlDaoHelper(dataSource, new IApplicationMetrics.NoOpApplicationMetrics());
    }
    return mysqlDaoHelper;
  }

  public static void createCheckPointRow(String instanceName, long dummyTimeStamp) {
    getMysqlDaoHelper().persist(
        "Create the checkpointing for first time: ",
        "insert into scylla_kafka_checkpointing.scylla_checkpoint (instance_name,last_read_cdc_timestamp) values(?,?)",
        statement -> {
          statement.setString(1, instanceName);
          statement.setLong(2,dummyTimeStamp);
        });
  }

  public static boolean updateCheckPoint(String instanceName,long readTimeStamp) {
    return getMysqlDaoHelper().update(
        "Updating the checkpointing for the scylla: ",
        "update scylla_kafka_checkpointing.scylla_checkpoint set last_read_cdc_timestamp = ? where instance_name =?",
        statement -> {
          statement.setLong(1, readTimeStamp);
          statement.setString(2, instanceName);

        });
  }

  public static CheckPointDetails getCheckPointDetails(String keyspaceTableCombination){
    String query = "Select keyspace_table_combination,last_read_cdc_timestamp from scylla_kafka_checkpointing.scylla_checkpoint where keyspace_table_combination= ?";
    return getMysqlDaoHelper().findOne(
        "Checkpointing_Get_Details",
        query,
        statement -> statement.setString(1, keyspaceTableCombination),
        new CheckPointMapper(),
        CheckPointDetails.class
    ).orElse(null);
  }

  private static final class CheckPointMapper implements IMysqlDaoHelper.RowMapper<CheckPointDetails> {
    @Override
    public CheckPointDetails map(ResultSet resultSet) throws SQLException {
      CheckPointDetails checkPointDetails = new CheckPointDetails();
      checkPointDetails.setKeySpaceTableNameCombination(resultSet.getString("keyspace_table_combination"));
      checkPointDetails.setLastReadTimestamp(resultSet.getLong("last_read_cdc_timestamp"));
      return checkPointDetails;
    }
  }
}
