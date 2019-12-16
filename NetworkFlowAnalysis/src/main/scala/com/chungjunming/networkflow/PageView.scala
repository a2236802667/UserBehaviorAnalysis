package com.chungjunming.networkflow

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by Chungjunming on 2019/10/25.
  */

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

object PageView {
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
      .map(userBehavior => {
        (userBehavior.behavior, 1)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

    resultStream.print("Page View")

    env.execute()
  }
}
