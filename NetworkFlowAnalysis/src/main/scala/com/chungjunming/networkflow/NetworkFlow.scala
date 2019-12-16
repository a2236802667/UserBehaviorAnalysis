package com.chungjunming.networkflow

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


/**
  * Created by Chungjunming on 2019/10/25.
  */

case class ApacheLogEvent(id: String, userId: String, eventTime: Long, method: String, url: String)

case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlow {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    val inputStream = env.readTextFile("D:\\IDEA\\workspace\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
    val inputStream = env.socketTextStream("hadoop102", 7777)
    val dataStream = inputStream.map(data => {
      val dataArray = data.split(" ")
      val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timestamp = simpleDateFormat.parse(dataArray(3)).getTime
      ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = {
          element.eventTime
        }
      })

    val aggrStream = dataStream.filter(data => {
      val pattern = "^((?!\\.(css|js)$).)*$".r
      (pattern findFirstIn data.url).nonEmpty
    })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .aggregate(new AggrFunction(), new WindowCountFunction())

    val processStream = aggrStream.keyBy(_.windowEnd).process(new TopNHotUrls(5))

    inputStream.print("input")
    aggrStream.print("aggr")
    processStream.print("process")

    env.execute("TopNHotUrls")

  }
}

class AggrFunction() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class WindowCountFunction() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

  //  lazy val listState: ListState[UrlViewCount] = getRuntimeContext.getListState[UrlViewCount](new ListStateDescriptor[UrlViewCount]("urlMap-state", classOf[UrlViewCount]))

  lazy val mapState: MapState[String, Long] = getRuntimeContext.getMapState[String, Long](new MapStateDescriptor[String, Long]("map-state", classOf[String], classOf[Long]))


  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    mapState.put(value.url, value.count)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val listBuffer = ListBuffer[(String,Long)]()

    val it = mapState.entries().iterator()

    while (it.hasNext) {
      val tuple = it.next()
      listBuffer += ((tuple.getKey,tuple.getValue))
    }
    mapState.clear()
    val sortedItems = listBuffer.sortWith(_._2 > _._2).take(topSize)

    val results = new StringBuilder()
    results.append("时间:").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- sortedItems.indices) {
      val currentItem = sortedItems(i)
      results.append("No").append(i + 1).append(":")
        .append("URL=").append(currentItem._1)
        .append("点击量=").append(currentItem._2)
        .append("\n")
    }
    results.append("==================================")
    Thread.sleep(1000L)

    out.collect(results.toString())
  }
}


