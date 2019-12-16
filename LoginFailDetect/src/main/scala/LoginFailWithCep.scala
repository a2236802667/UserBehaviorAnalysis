import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by Chungjunming on 2019/10/28.
  */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginLog.csv")
    val dataStream = env.readTextFile(resource.getPath)

    val keyStream = dataStream.map(data => {
      val dataArray = data.split(",")
      LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })
      .keyBy(_.userId)

    val pattern = Pattern.begin[LoginEvent]("start").where(_.status == "fail")
      .next("next").where(_.status == "fail")
      .within(Time.seconds(2))
    val patternStream = CEP.pattern(keyStream,pattern)

    patternStream.select(new LoginFailDetect())


  }
}

class LoginFailDetect() extends PatternSelectFunction[LoginEvent,Warning] {
  override def select(pattern: util.Map[String, util.List[LoginEvent]]): Warning = {
    val firstFailEvent = pattern.get("start").iterator().next()
    val secondFailEvent = pattern.get("next").iterator().next()
    Warning(firstFailEvent.userId,firstFailEvent.eventTime,secondFailEvent.eventTime,"login fail 2 times")
  }
}