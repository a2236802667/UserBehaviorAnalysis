package com.chungjunming.networkflow

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis


/**
  * Created by Chungjunming on 2019/10/25.
  */
object UvWithBloom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("D:\\IDEA\\workspace\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val dataStream = inputStream.map(data => {
      val dataArray = data.split(",")
      UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val resultStream = dataStream.filter(_.behavior == "pv")
      .map(data => {
        ("uv", data.userId)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      //      .trigger(new MyTrigger())
      //            .trigger(CountTrigger.of(10000))
      .trigger(new CouTrriger())
      .process(new ProcessUserCountFunc())


    resultStream.print("process")

    env.execute("Unique Visitor With Bloom")
  }
}

class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {

  }
}

class Bloom(size: Long) extends Serializable {
  private val cap = size

  def hash(value: String, seed: Int): Long = {
    //    MurmurHash3.stringHash(value)
    var result = 0
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    (cap - 1) & result
  }
}


class ProcessUserCountFunc() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
  lazy val jedis = new Jedis("hadoop102", 6379)
  lazy val bloom = new Bloom(1 << 27)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {

    val windowEnd = context.window.getEnd.toString

    var count = 0L

    if (jedis.hget("count", windowEnd) != null) {
      count = jedis.hget("count", windowEnd).toLong
    }

    val userId = elements.last._2
    val offset = bloom.hash(userId.toString, 59)

    val isExists = jedis.getbit(windowEnd.toString, offset)

    if (!isExists) {
      jedis.setbit(windowEnd, offset, true)
      jedis.hset("count", windowEnd, (count + 1).toString)
      out.collect(UvCount(windowEnd.toLong, count + 1))
    }
  }
}

class CouTrriger() extends Trigger[(String, Long), TimeWindow] {

  val batchState = new ValueStateDescriptor[Long]("sum", classOf[Long])

  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    val sumState = ctx.getPartitionedState(batchState)
    if(sumState.value() == 0L){
      sumState.update(0L);
    }
    sumState.update(sumState.value() + 1L)
    if(sumState.value() >= 100L){
      TriggerResult.FIRE_AND_PURGE
    }else{
      TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {

  }
}
