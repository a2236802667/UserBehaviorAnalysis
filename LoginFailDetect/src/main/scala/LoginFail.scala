import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.{TimeCharacteristic, functions}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Created by Chungjunming on 2019/10/27.
  */

case class LoginEvent(userId: Long, ip: String, status: String, eventTime: Long)

case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginLog.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })
      .keyBy(_.userId)
      //      .process(new LoginFailWarning(2))
      .process(new LoginFailWarningAdv(2))

    dataStream.print()
    env.execute("login fail job")
  }
}

class LoginFailWarning(failTimes: Int) extends KeyedProcessFunction[Long,LoginEvent,Warning] {
  lazy val loginFailListsState = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginFailList-state",classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
      if(value.status == "fail"){
        loginFailListsState.add(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 2000L)
      }else{
        loginFailListsState.clear()
      }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    import scala.collection.JavaConversions._
    val times = loginFailListsState.get().size
    if(times >= failTimes){
      out.collect(Warning(ctx.getCurrentKey,
        loginFailListsState.get().head.eventTime,
        loginFailListsState.get().last.eventTime,
      "Login fail in 2 seconds for" + times + "times"))
    }
    loginFailListsState.clear()
  }
}

class LoginFailWarningAdv(failTimes: Int) extends KeyedProcessFunction[Long,LoginEvent,Warning] {
  lazy val loginFailListsState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginFailList-state", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    if(value.status == "fail"){
      val iter = loginFailListsState.get().iterator()
      if(iter.hasNext){
        val firstFailEvent = iter.next()
        if((value.eventTime - firstFailEvent.eventTime).abs < 5){
          out.collect(Warning(value.userId,firstFailEvent.eventTime,value.eventTime,"login fail in 2 seconds"))
        }
        loginFailListsState.clear()
        loginFailListsState.add(value)
      }else{
        loginFailListsState.add(value)
      }
    }else{
      loginFailListsState.clear()
    }
  }
}