package cn.lmx.loginfail_detect

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

//输入数据的样例类
case class LoginLog(userId: Long, ip: String, eventType: String, timestamp: Long)

//输出数据的样例类
case class LonginFailWarning(userId: Long, firstTime: Long, secondTime: Long, warningMasge: String)

/**
 * 恶意登录监测
 */
object LoginFail {
  def main(args: Array[String]): Unit = {
    //环境
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setParallelism(1)
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val sourceStream: DataStream[String] = senv.readTextFile("datas\\LoginLog.csv")
    val loginStream: DataStream[LoginLog] = sourceStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        LoginLog(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginLog](Time.seconds(3)) {
        override def extractTimestamp(element: LoginLog): Long = {
          element.timestamp * 1000
        }
      })
    //进行判断检测，如果2s内连续登录失败2次，输出报警信息
    //同一个人连续登录失败算作恶意登录，首先按照userId分组
    loginStream.keyBy(_.userId)
      .process(new LoginFailWarningResult(2))
      .print()
    senv.execute()
  }
}

class LoginFailWarningResult(failTimes: Int) extends KeyedProcessFunction[Long, LoginLog, LonginFailWarning]() {

  //定义状态，保存当前所有的登录事件，保存定时器时间戳
  var loginFailListState: ListState[LoginLog] = _
  var timerState: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    loginFailListState = getRuntimeContext.getListState(new ListStateDescriptor[LoginLog]("longinfail-list", classOf[LoginLog]))
    timerState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))
  }

  override def processElement(value: LoginLog, ctx: KeyedProcessFunction[Long, LoginLog, LonginFailWarning]#Context, out: Collector[LonginFailWarning]): Unit = {
    //判断当前登录事件是成功还是失败
    if (value.eventType == "fail") {
      loginFailListState.add(value)
      //如果没有定时器，那么注册一个2s后的定时器
      if (timerState.value() == 0) {
        val ts: Long = value.timestamp * 1000L + 2000L
        timerState.update(ts)
        ctx.timerService().registerEventTimeTimer(ts)
      }
    } else {
      //登录成功，清空状态信息和定时器
      ctx.timerService().deleteEventTimeTimer(timerState.value())
      loginFailListState.clear()
      timerState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginLog, LonginFailWarning]#OnTimerContext, out: Collector[LonginFailWarning]): Unit = {
    //从失败状态中取出失败事件
    import scala.collection.mutable.ListBuffer
    val it: util.Iterator[LoginLog] = loginFailListState.get().iterator()
    var allFailList: ListBuffer[LoginLog] = ListBuffer()
    while (it.hasNext) {
      allFailList += it.next()
    }
    //判断登录失败时间个数，如果超过设定次数，输出报警信息
    if (allFailList.length >= failTimes) {
      out.collect(
        LonginFailWarning(
          allFailList.head.userId,
          allFailList.head.timestamp,
          allFailList.last.timestamp,
          "说你又有不听，听你又不做，做了又做错，错了你也不认，认了还不改，改了也不服"
        )
      )
    }
    //清空状态
    loginFailListState.clear()
    timerState.clear()
  }
}
