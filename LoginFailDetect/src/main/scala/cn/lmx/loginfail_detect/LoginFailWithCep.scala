package cn.lmx.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 恶意登录监测
 */
object LoginFailWithCep {
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
    //1.定义匹配模式，要求是一个登录失败后，紧跟另一个登录失败事件
    val loginFailPattern: Pattern[LoginLog, LoginLog] = Pattern
      //第一次失败事件的名称
      .begin[LoginLog]("firstFail").where(_.eventType == "fail")
      .next("secondFail").where(_.eventType == "fail")
      .next("thirdFail").where(_.eventType == "fail")
      //要求时间在2s内
      .within(Time.seconds(3))
    //2.将模式应用到数据流，得到一个PatternStream
    val warningMasge: DataStream[LonginFailWarning] = CEP.pattern(loginStream.keyBy(_.userId), loginFailPattern)
      //3.检出符合模式的数据流，需要调用select
      .select(new LoginFailEventMath())
    warningMasge.print()
    senv.execute()
  }
}

class LoginFailEventMath() extends PatternSelectFunction[LoginLog, LonginFailWarning] {
  //匹配到的事件，保存在map里
  override def select(map: util.Map[String, util.List[LoginLog]]): LonginFailWarning = {
    val firstFailEvent: LoginLog = map.get("firstFail").iterator().next()
//    val secondFailEvent: LoginLog = map.get("secondFail").iterator().next()
    val thirdFailEvent: LoginLog = map.get("thirdFail").iterator().next()
    LonginFailWarning(firstFailEvent.userId, firstFailEvent.timestamp, thirdFailEvent.timestamp, "login fail")
  }
}


