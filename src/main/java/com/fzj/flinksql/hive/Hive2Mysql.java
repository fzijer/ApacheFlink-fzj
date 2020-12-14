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


        String sql =
                "create table testOut ( " +
                        "name varchar(20) not null "+
                        ") with ( "+
                        "'connector.type' = 'jdbc',"+
                        "'connector.url' = 'jdbc:mysql://120.79.161.133:3306/test?characterEncoding=UTF-8',"+
                        "'connector.table' = 'test_stu',"+
                        "'connector.driver' = 'com.mysql.jdbc.Driver',"+
                        "'connector.username' = 'root',"+
                        "'connector.password' = 'xwj$wsd98')";
        tableEnv.executeSql(sql);
        statementSet.addInsert("testOut",sqlResult);


        statementSet.execute();

    }

}
