package com.chungjunming.market

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * Created by Chungjunming on 2019/10/25.
  */

case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

// 定义一个输出样例类
case class MarketingViewCount(windowStart: String, channel: String, behavior: String, count: Long)

// 自定义source
class SimulatedEventSource extends RichSourceFunction[MarketingUserBehavior] {

  var running = true

  val channelSet = Seq("AppStore", "HuaweiStore", "XiaomiStore", "weibo", "wechat")

  val behaviorTypes = Seq("CLICK", "DOWNLOAD", "UPDATE", "INSTALL", "UNINSTALL")

  val rand = Random

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    val max_Count = Long.MaxValue
    var count = 0L
    while (running && count < max_Count){
      val id = UUID.randomUUID().toString
      val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()

      ctx.collect(MarketingUserBehavior(id, behavior, channel, ts))
      count += 1
      Thread.sleep(100L)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}


object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.timestamp)

    inputStream.filter(_.behavior != "UNINSTALL")
      .map(data => ((data.channel,data.behavior),1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1),Time.seconds(10))
      .process(new MarketingCountByChannel())
      .print()

    env.execute("app marketing by channel job")


  }
}

class MarketingCountByChannel() extends ProcessWindowFunction[((String,String),Long),MarketingViewCount,(String,String),TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
    val timestamp = new Timestamp(context.window.getStart)
    out.collect(MarketingViewCount(timestamp.toString,key._1,key._2,elements.size))
  }
}
