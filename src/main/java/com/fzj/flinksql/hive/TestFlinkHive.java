package com.fzj.flinksql.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;


/**
 * @author fzijer
 * @Commnet 使用FlinkSql 连接HIVE 读取数据
 */
public class TestFlinkHive {
  public static void main(String[] args) {
    /**1。Blink 的  Batch Query*/
    //EnvironmentSettings 执行环境   StreamExecutionEnvironment  流式执行环境
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().useBlinkPlanner().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);

    //tableEnv
    /**2.设定 hive 连接属性*/
    String name = "myhive";
    /* 选择 hive里面的 Database*/
    String defaultDatabase = "default";
    String hiveConfDir = "src\\main\\resources";

    /**3.创建 HiveCatalog*/
    HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
    tableEnv.registerCatalog("myhive", hive);
    tableEnv.useCatalog("myhive");

   //可以进行Join等操作
    String sql = "select name from t";
    //得到Table对象
    Table table = tableEnv.sqlQuery(sql);

    tableEnv.insertInto("test.txt",table);
    TableResult execute = table.execute();
    //execute.print();
    System.out.println(execute.getJobClient());
    System.out.println(execute.getResultKind());
    System.out.println(execute.getTableSchema());
    //处理Table对象
    execute.collect().forEachRemaining(row -> {
      //System.out.println(row);
      //System.out.println(row.getKind());  /*输出类型*/
      //System.out.println(row.getArity()); /*输出列的长度*/
      //System.out.println(row.getField(0));
    });




    // set the HiveCatalog as the current catalog of the session

  }
}
