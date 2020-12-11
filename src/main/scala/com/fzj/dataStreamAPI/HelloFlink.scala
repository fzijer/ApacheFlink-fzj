/*
package com.fzj.dataStreamAPI

import java.util.Collections
import java.{lang, util}

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.{IterationRuntimeContext, MapFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.{TimeCharacteristic, environment}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HelloFlink {
  def main(args: Array[String]): Unit = {
    //1.创建一个本地的流式执行环境
    val localEnv: StreamExecutionEnvironment =StreamExecutionEnvironment.createLocalEnvironment()

    //创建一个远程的流式执行环境
    //val remoteEnv: StreamExecutionEnvironment =StreamExecutionEnvironment.createRemoteEnvironment("host",1234,"path/to/jarFile.jar")

    //2.指定程序采用事件时间语义
    localEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sensorData: DataStream[SensorReading] =localEnv.addSource(new SensorSource)

    //3.使用map对应用进行数据转换
    val vagTemp =sensorData/*.map(r =>{
      val celsius =(r.temperature-32)*(5.0/9.0)                     // 将温度进行转换
      SensorReading(r.id,r.timestamp,celsius)                       //将每一条数据输出为该数据类型

    })*/
        .map(new MyMapFunction)                                     //使用自定义map函数
        .keyBy(_.id) //按照 每条数据的 id进行分组
        .sum("temperature")

//      .timeWindow(Time.seconds(5))                        // 将窗口的长度设置为5秒钟

    println(vagTemp);
    sensorData.print();
    localEnv.execute("执行Flink程序")

  }


  /**自定义mapfunction
   * */
   class MyMapFunction extends MapFunction[SensorReading,SensorReading]{

    override def map(t: SensorReading): SensorReading = {

      SensorReading("id1",23,23);
    }

  }


  /**自定义富函数*/
  class MyRIchFunction extends RichMapFunction[SensorReading,SensorReading]{
    override def map(value: SensorReading): SensorReading = {
      
      SensorReading("id1",value.timestamp,value.temperature+1);
    }

    override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)

    override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

    override def getIterationRuntimeContext: IterationRuntimeContext = super.getIterationRuntimeContext

    override def open(parameters: Configuration): Unit = super.open(parameters)

    override def close(): Unit = super.close()
  }


  /**
   *  自定义处理函数 ProcessFunction
   *  <I> Type of the input elements.
   *  <O> Type of the output elements.
   * */
  class MyProcessFunction extends  ProcessFunction[SensorReading,SensorReading]{

    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = ???

    override def onTimer(timestamp: Long, ctx: ProcessFunction[SensorReading, SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = super.onTimer(timestamp, ctx, out)

    override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)

    override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

    override def getIterationRuntimeContext: IterationRuntimeContext = super.getIterationRuntimeContext

    override def open(parameters: Configuration): Unit = super.open(parameters)

    override def close(): Unit = super.close()


  }


  /**
   * 自定义处理函数KeyedProdessFunction
   * <K> Type of the key.
   * <I> Type of the input elements.
   * <O> Type of the output elements.
   * */
  class MyKeyedProcessFunction extends KeyedProcessFunction[String,SensorReading,SensorReading]{
    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {

      SensorReading("id1",23,23);
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = super.onTimer(timestamp, ctx, out)

    override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)

    override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

    override def getIterationRuntimeContext: IterationRuntimeContext = super.getIterationRuntimeContext

    override def open(parameters: Configuration): Unit = super.open(parameters)

    override def close(): Unit = super.close()
  }


  /**
   * 自定义全窗口函数 (处理函数中的一个)    IN, OUT, KEY, W <: Window>
   * */
  class MyProcessWindwoFunction extends  ProcessWindowFunction[ SensorReading, SensorReading,String,TimeWindow]{
    override def process(key: String, context: ProcessWindowFunction[SensorReading, SensorReading, String, TimeWindow]#Context, elements: lang.Iterable[SensorReading], out: Collector[SensorReading]): Unit = ???

    override def clear(context: ProcessWindowFunction[SensorReading, SensorReading, String, TimeWindow]#Context): Unit = super.clear(context)

    override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)

    override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

    override def getIterationRuntimeContext: IterationRuntimeContext = super.getIterationRuntimeContext

    override def open(parameters: Configuration): Unit = super.open(parameters)

    override def close(): Unit = super.close()

  }


  /**自定义 窗口分配器
   * <T> 此WindowAssigner可以将窗口分配给的元素类型.
   * <W> 窗口类型.
   * */
  class MyWindowAssigner extends WindowAssigner[Object,TimeWindow]{

    override def assignWindows(element: Object, timestamp: Long, context: WindowAssigner.WindowAssignerContext): util.Collection[TimeWindow] = {

      val windowSize: Long =3*1000L   //定义窗口长度
      val startTime = timestamp -(timestamp%windowSize)
      val endTime = startTime +windowSize
      Collections.singletonList(new TimeWindow(startTime, endTime))
    }

    override def getDefaultTrigger(env: environment.StreamExecutionEnvironment): Trigger[Object, TimeWindow] = EventTimeTrigger.create()

    override def getWindowSerializer(executionConfig: ExecutionConfig): TypeSerializer[TimeWindow] = new TimeWindow.Serializer

    override def isEventTime: Boolean =  true

  }



  /**
   * 自定义数据源
   * 重写里面的 RUN方法　　和　cancel 方法*/
  class MySourceFunction extends SourceFunction[Long]{

    var isRunning =true

    //在该方法中造数据，并输出数据
    override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
      var cnt = -1L
        while(isRunning && cnt<Long.MaxValue ){
          cnt+=1
          ctx.collect(cnt)
        }
    }

    override def cancel(): Unit = {
      isRunning=false
    }
  }

}



*/
