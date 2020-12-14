package com.fzj.flinksql.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;


/**
 * @author fzijer
 * @Comment 从hive中读取数据后,将数据写入到 Kafka 中
 */
public class Hive2Kafka {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);
        // Catalog名称，定义一个唯一的名称表示
        String name = "myhive";
        // 默认数据库名称
        String defaultDatabase = "default";
        // hive-site.xml路径
        String hiveConfDir = "src\\main\\resources";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        StatementSet statementSet = tableEnv.createStatementSet();

        tableEnv.registerCatalog(name, hive);
        tableEnv.useCatalog(name);
        Table sqlResult = tableEnv.sqlQuery("select name from t");

        //先删除表
        tableEnv.executeSql("drop table flink_sink_t");

        String sql =
                "CREATE TABLE flink_sink_t (" +
                        "  name String" +
                        ") WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = 'flink_sink_t'," +
                        "  'properties.bootstrap.servers' = '120.79.161.133:9092'," +
                        "  'format' = 'json'" +
                        ")";
        //执行后 hive中 建立 flink_sink_t空表
        tableEnv.executeSql(sql);
        statementSet.addInsert("flink_sink_t", sqlResult);
        statementSet.execute();
    }
}