package com.fzj.flink.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;


public class TestFlinkHive {
  public static void main(String[] args) {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().useBlinkPlanner().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);

    String name = "myhive";
    /* 选择 hive里面的 Database*/
    String defaultDatabase = "default";
    String hiveConfDir = "src\\main\\resources";

    //创建 HiveCatalog
    HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
    tableEnv.registerCatalog("myhive", hive);

// set the HiveCatalog as the current catalog of the session
    tableEnv.useCatalog("myhive");


    String sql = "select name from t";
    Table table = tableEnv.sqlQuery(sql);
    TableResult execute = table.execute();
    execute.collect().forEachRemaining(row -> {
      System.out.println(row.getField(0));
    });
  }
}
