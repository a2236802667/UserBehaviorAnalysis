package com.chungjunming.networkflow

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by Chungjunming on 2019/10/25.
  */
case class UvCount(windowEnd: Long, count: Long)

object UniqueVisitor {
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
      .timeWindowAll(Time.hours(1))
        .process(new ProcessUserCount())

    resultStream.print("process")

    env.execute("Unique Visitor")
  }
}

class ProcessUserCount() extends ProcessAllWindowFunction[UserBehavior,UvCount,TimeWindow] {
  override def process(context: Context, elements: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {

    var set = Set[Long]()

    for(userBehavior <- elements){
      set += userBehavior.userId
    }

      out.collect(UvCount(context.window.getEnd,set.size))
  }
}





