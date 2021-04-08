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

/**
 * 恶意登录监测
 */
object LoginFailAdvance {
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
      .process(new LoginFailWarningResultAdvance(2))
      .print()
    senv.execute()
  }
}

class LoginFailWarningResultAdvance(failTimes: Int) extends KeyedProcessFunction[Long, LoginLog, LonginFailWarning] {
  //定义状态，保存当前所有的登录事件，保存定时器时间戳
  var loginFailListState: ListState[LoginLog] = _
  var timerState: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    loginFailListState = getRuntimeContext.getListState(new ListStateDescriptor[LoginLog]("longinfail-list", classOf[LoginLog]))
    timerState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))
  }

  override def processElement(value: LoginLog, ctx: KeyedProcessFunction[Long, LoginLog, LonginFailWarning]#Context, out: Collector[LonginFailWarning]): Unit = {
    //1.判断当前登录事件是成功还是失败
    if (value.eventType == "fail") {
      //1.1如果是失败，就再次判断
      //判断状态中是否有登录失败事件
      val it: util.Iterator[LoginLog] = loginFailListState.get().iterator()
      if (it.hasNext) {
        //1.1.1如果有,继续判断两次失败的时间差是否在2s以内
        val firstFailEvent = it.next()
        if (value.timestamp < value.timestamp + 2) {
          //如果在2s之内，输出报警
          out.collect(
            LonginFailWarning(
              value.userId,
              firstFailEvent.timestamp,
              value.timestamp,
              "login fail 2 times in 2s")
          )
        }
        //不管报不报警，当前都已经处理完毕，把最近失败信息更新到状态中
        loginFailListState.clear()
        loginFailListState.add(value)
      } else {
        //1.2如果没有，直接把当前事件添加到状态中
        loginFailListState.add(value)
      }
    }
    else {
      //2.如果是成功，清空状态
      loginFailListState.clear()
    }
  }
}


