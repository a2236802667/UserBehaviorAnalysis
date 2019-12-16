package com.chungjunming.orderpay

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Created by Chungjunming on 2019/10/28.
  */
object TxMatchWithJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val receiptResource = getClass.getResource("/ReceiptLog.csv")

    val receiptEventStream = env.readTextFile(receiptResource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    val orderResource = getClass.getResource("/OrderLog.csv")
    val orderStream = env.readTextFile(orderResource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    orderStream.join(receiptEventStream).where(_.txId).equalTo(_.txId).window(TumblingEventTimeWindows.of(Time.seconds(15)))
      .apply((orderEvent, receiptEvent) => (orderEvent, receiptEvent))
      .print()

    val processJoinStream = orderStream.keyBy(_.txId).intervalJoin(receiptEventStream).between(Time.seconds(-20), Time.seconds(15))
      .process(new MyProcessJoinFunc())

    processJoinStream.print()

    env.execute("TX Match With Join")

  }
}

class MyProcessJoinFunc() extends ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)] {
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((left,right))
  }
}
