package com.chungjunming.market

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by Chungjunming on 2019/10/25.
  */
object AppMarketing {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.addSource( new SimulatedEventSource )
      .assignAscendingTimestamps(_.timestamp)

    inputStream.filter(_.behavior != "UNINSTALL")
      .map(data => ("dummyKey",1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1),Time.seconds(10))
      .process(new MarketingCountTotal())
      .print()

    env.execute("app marketing job")



  }
}

class MarketingCountTotal() extends ProcessWindowFunction[(String,Long),MarketingViewCount,String,TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[MarketingViewCount]): Unit = {
    val timestamp = new Timestamp(context.window.getStart)
    out.collect(MarketingViewCount(timestamp.toString,"total channel","total behavior",elements.size))
  }
}
