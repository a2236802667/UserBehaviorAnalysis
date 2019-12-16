package com.chungjunming

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Created by Chungjunming on 2019/10/24.
  */
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object hotitems_analysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

        val inputStream = env.readTextFile("D:\\IDEA\\workspace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    //    val inputStream = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
//    val inputStream = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
    val aggrStream = inputStream.map {
      lines => {
        val dataArray = lines.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      }
    }.assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new AggerFunction(), new WindowCountFunction())

    val processedStream = aggrStream.keyBy(_.windowEnd)
      .process(new TopNCountProcessFunction(3))
//        inputStream.print("input")
//    aggrStream.print("aggr")
    processedStream.print("process")

    env.execute("Hot Items Job")

  }
}

class AggerFunction() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class WindowCountFunction() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNCountProcessFunction(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  var listState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    listState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("listState", classOf[ItemViewCount]))
  }


  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    listState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val listBuffer = ListBuffer[ItemViewCount]()
    import scala.collection.JavaConversions._
    for (item <- listState.get()) {
      listBuffer += item
    }
    listState.clear()
    val sortedItems = listBuffer.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    val results = new StringBuilder()
    results.append("时间:").append(new Timestamp(timestamp)).append("\n")

    for (i <- sortedItems.indices) {
      val currentItem = sortedItems(i)
      results.append("No").append(i + 1).append(":")
        .append("商品ID=").append(currentItem.itemId)
        .append("点击量=").append(currentItem.count)
        .append("\n")
    }
    results.append("==================================")
    Thread.sleep(1000L)

    out.collect(results.toString())
  }
}
