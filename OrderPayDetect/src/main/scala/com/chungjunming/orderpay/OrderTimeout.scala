package com.chungjunming.orderpay

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by Chungjunming on 2019/10/28.
  */
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/OrderLog.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    val pattern = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay").where(_.txId != "")
      .within(Time.minutes(15))

    val outputTag = new OutputTag[OrderResult]("orderTimeout")

    val PatternStream = CEP.pattern(dataStream,pattern)

    val resultStream = PatternStream.select(outputTag,new OrderTimeoutFunc(),new OrderFunc())

    resultStream.print("payed")
    resultStream.getSideOutput(outputTag).print("timeout")

    env.execute("order time out job")
  }
}

class OrderTimeoutFunc() extends PatternTimeoutFunction[OrderEvent,OrderResult] {
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
      OrderResult(pattern.get("begin").iterator().next().orderId,"time out")
  }
}

class OrderFunc() extends PatternSelectFunction[OrderEvent,OrderResult] {
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    OrderResult(pattern.get("follow").iterator().next().orderId,"payed successfully")
  }
}
