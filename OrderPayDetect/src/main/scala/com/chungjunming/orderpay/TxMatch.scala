package com.chungjunming.orderpay

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Created by Chungjunming on 2019/10/28.
  */
case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)


object TxMatch {
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

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

    val connectedStream = orderStream.connect(receiptEventStream)

    val processedStream = connectedStream.process(new CoProcessTx())

    processedStream.getSideOutput(unmatchedPays).print("unmatchedPays")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")
    processedStream.print("matched")

    env.execute("Tx Match")
  }
  class CoProcessTx() extends CoProcessFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)] {
    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("orderEvent-state",classOf[OrderEvent]))
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receiptEvent-state",classOf[ReceiptEvent]))

    override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val receipt = receiptState.value()
      if(receipt != null){
        out.collect((value,receipt))
        receiptState.clear()
        payState.clear()
      }else{
        payState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5000L)
      }
    }

    override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val pay = payState.value()
      if(pay != null){
        out.collect((pay,value))
        receiptState.clear()
        payState.clear()
      }else{
        receiptState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 3000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      if(payState.value() != null){
        ctx.output(unmatchedPays,payState.value())
      }
      if(receiptState.value() != null){
        ctx.output(unmatchedReceipts,receiptState.value())
      }
      payState.clear()
      receiptState.clear()
    }
  }
}

