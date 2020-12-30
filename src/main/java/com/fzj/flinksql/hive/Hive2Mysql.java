package com.fzj.flinksql.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author fzijer
 * @Comment 从hive中读取数据后，并将处理后的数据写入到Mysql中。
 */
public class Hive2Mysql {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();

//        EnvironmentSettings settings = EnvironmentSettings
//                .newInstance()
//                .useBlinkPlanner()
//                .inBatchMode()
//                .build();

        TableEnvironment tableEnv = TableEnvironment.create(fsSettings);
        // Catalog名称，定义一个唯一的名称表示
        String name = "hive";
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
        tableEnv.executeSql("drop table testOut4");
        System.out.println("23");
        String sql =
                "create table testOut4 ( " +
                        "name varchar(20) not null "+
                        ") with ( "+
                        "'connector.type' = 'jdbc',"+
                        "'connector.url' = 'jdbc:mysql://192.168.18.158:3306/test?characterEncoding=UTF-8',"+
                        "'connector.table' = 't',"+
                        "'connector.driver' = 'com.mysql.jdbc.Driver',"+
                        "'connector.username' = 'root',"+
                        "'connector.password' = 'root')";
        tableEnv.executeSql(sql);
        statementSet.addInsert("testOut",sqlResult);


        statementSet.execute();

    }

}
