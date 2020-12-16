package com.fzj.FlinkSQL

/**四种 不同的Flink SQL  环境 */
object CreateTableEnvironment {
  def main(args: Array[String]): Unit = {


   /**
    * Flink  Streaming  Query*/
    import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
    import org.apache.flink.table.api.EnvironmentSettings
    import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

    val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()    //使用旧计划器，并使用StreamingMode
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment                                  //创建Streaming 的执行环境
    val fsTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(fsEnv, fsSettings)       //根据流执行环境和 计划器 来创建 StreamTableEnvironment表环境
  // or val fsTableEnv: TableEnvironment = TableEnvironment.create(fsSettings)



    /**
     * Flink  的 Batch Query*/
    import org.apache.flink.api.scala.ExecutionEnvironment
    import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment

    val fbEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment                                        //创建Batch 的执行环境
    val fbTableEnv = BatchTableEnvironment.create(fbEnv)                                            //根据批次执行环境 创建BatchTableEnvironment 表环境
                                                                                                 // Batch执行环境 默认使用  旧计划器



    /**Blink 的 Streaming Query
     * */
    import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
    import org.apache.flink.table.api.EnvironmentSettings
    import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

    val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment                                  // 创建Streaming的执行环境
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()  //使用 Blink的计划器
    val bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings)                               //根据Streaming执行环境 和  Blink计划器 来创建StreamTableEnvironment表环境
    // or val bsTableEnv = TableEnvironment.create(bsSettings)



    /**Blink 的  Batch Query
     * */
    import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

    val bbSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()      //使用 Blink的 计划器,并使用 Batch执行环境
    val bbTableEnv: TableEnvironment = TableEnvironment.create(bbSettings)
  }

}
