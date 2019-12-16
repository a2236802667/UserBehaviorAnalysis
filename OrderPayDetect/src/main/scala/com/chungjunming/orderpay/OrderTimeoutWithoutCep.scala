package com.chungjunming.orderpay

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Created by Chungjunming on 2019/10/28.
  */
object OrderTimeoutWithoutCep {

  val orderTimerOutputTag = new OutputTag[OrderResult]("orderTimeOut")


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val resource = getClass.getResource("/OrderLog.csv")
    //    val orderEventStream = env.readTextFile(resource.getPath)
    val orderEventStream = env.socketTextStream("hadoop102", 7777)
    val KeyedStream = orderEventStream.map(data => {
      val dataArray = data.split(",")
      OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
    })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    val resultStream = KeyedStream.process(new OrderPayMatch())

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimerOutputTag).print("timeOut")

    env.execute("order time out without cep")
  }

  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("payed-state", classOf[Boolean]))
    lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state", classOf[Long]))

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      if (timerState.value() == timestamp) {
        if (isPayedState.value()) {
          ctx.output(orderTimerOutputTag, OrderResult(ctx.getCurrentKey, "alreary payed but not found create"))
        } else {
          ctx.output(orderTimerOutputTag, OrderResult(ctx.getCurrentKey, "payed time out"))
        }
        isPayedState.clear()
        timerState.clear()
      }
    }

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      val isPayed = isPayedState.value()
      val timerTS = timerState.value()

      if (value.eventType == "create") {
        if (isPayed) {
          out.collect(OrderResult(value.orderId, "payed successfully"))
          ctx.timerService().deleteEventTimeTimer(timerTS)
          isPayedState.clear()
          timerState.clear()
        } else {
          val ts = value.eventTime * 1000L + 900 * 1000L
          ctx.timerService().registerEventTimeTimer(ts)
          timerState.update(ts)
        }
      } else if (value.eventType == "pay") {
        if (timerTS > 0) {
          if (value.eventTime * 1000L < timerTS) {
            out.collect(OrderResult(value.orderId, "payed successfully"))
          } else {
            ctx.output(orderTimerOutputTag, OrderResult(value.orderId, "payed but already timeout"))
          }
          ctx.timerService().deleteEventTimeTimer(timerTS)
          isPayedState.clear()
          timerState.clear()
        } else {
          isPayedState.update(true)
          ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L)
          timerState.update(value.eventTime * 1000L)
        }
      }
    }
  }

}
