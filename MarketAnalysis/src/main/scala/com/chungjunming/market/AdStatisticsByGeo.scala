package com.chungjunming.market

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by Chungjunming on 2019/10/26.
  */
// 输入数据样例类
case class AdClickEvent(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

// 输出按照省份划分的点击统计结果
case class AdCountByProvince(windowEnd: String, province: String, count: Long)

// 侧输出流的黑名单报警信息样例类
case class BlackListWarning(userId: Long, adId: Long, msg: String)

object AdStatisticsByGeo {
  val blackListOutputTag = new OutputTag[BlackListWarning]("blackList")


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/AdClickLog.csv")
    val inputStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        AdClickEvent(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

     inputStream.keyBy(data => (data.userId,data.adId))
      .process(new FilterBlackListUser(100))


    val aggStream = inputStream.keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(10))
      .aggregate(new AggrProCount(), new WindowCountByPro())

    aggStream.print("aggr")

    env.execute("ad count job")
  }

  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long,Long),AdClickEvent,AdClickEvent] {
    lazy val countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state",classOf[Long]))
    lazy val isSent = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isSend-state",classOf[Boolean]))
    lazy val resetTime = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resetTime-state",classOf[Long]))


    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      if(timestamp == resetTime.value()){
        isSent.clear()
        countState.clear()
      }
    }

    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      val curCount = countState.value()
      if(curCount == 0){
        val ts = ( ctx.timerService().currentProcessingTime()/(1000*60*60*24) + 1 ) * 24 * 60 * 60 * 1000L
        ctx.timerService().registerProcessingTimeTimer(ts)
        resetTime.update(ts)
      }
      countState.update(curCount + 1)
      if(curCount >= maxCount){
        if(!isSent.value()){
          ctx.output(blackListOutputTag,BlackListWarning(value.userId,value.adId,"Click over" + maxCount + "times today"))
          isSent.update(true
          )
        }
      }else{
        out.collect(value)
      }
    }
  }
}

class AggrProCount() extends AggregateFunction[AdClickEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class WindowCountByPro() extends WindowFunction[Long, AdCountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountByProvince]): Unit = {
    val windowEnd = new Timestamp(window.getEnd).toString
    out.collect(AdCountByProvince(windowEnd,key,input.iterator.next()))
  }
}


