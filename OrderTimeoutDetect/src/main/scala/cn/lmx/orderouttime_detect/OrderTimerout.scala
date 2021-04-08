package cn.lmx.orderouttime_detect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

//定义输入数据的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)

//定义输出数据的样例类
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimerout {
  def main(args: Array[String]): Unit = {

    //环境
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setParallelism(1)
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val sourceStream: DataStream[String] = senv.readTextFile("datas\\OrderLog.csv")
    val orderStream: DataStream[OrderEvent] = sourceStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.orderId)
    //1.定义一个pattern
    val orderPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("create")
      .where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))
    //2.将pattern应用到数据流上，进行模式检测
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderStream, orderPattern)
    //3.检出数据
    //调用select方法，提取并处理匹配成功的事件，及超时事件
    //定义一个侧输出流标签，用于处理超时事件
    val orderOutputTag = new OutputTag[OrderResult]("orderTimeout")
    val resultStream: DataStream[OrderResult] = patternStream.select(orderOutputTag, new OrderTimeoutSelect(), new OrderPaySelect())
    resultStream.print()
    resultStream.getSideOutput(orderOutputTag).print("timeout=>")
    senv.execute()
  }
}

class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val orderId: Long = map.get("pay").iterator().next().orderId
    OrderResult(orderId, "payed successfly")
  }
}

class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    //获取超时事件的订单id
    val orderId: Long = map.get("create").iterator().next().orderId
    OrderResult(orderId, "timeout:" + l)
  }
}
