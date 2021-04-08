package cn.lmx.orderouttime_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

//定义到账事件的样例类
case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)

object TxMatch {
  def main(args: Array[String]): Unit = {
    //1.环境
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setParallelism(1)
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.读取订单数据
    val sourceStream: DataStream[String] = senv.readTextFile("datas\\OrderLog.csv")
    val orderStream: KeyedStream[OrderEvent, String] = sourceStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.eventType == "pay").keyBy(_.txId)

    //3.读取到账数据
    val inputStream: DataStream[String] = senv.readTextFile("datas\\ReceiptLog.csv")
    val receiptStream: KeyedStream[ReceiptEvent, String] = inputStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        ReceiptEvent(arr(0), arr(1), arr(2).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L).keyBy(_.txId)

    //4.合并两条流，进行处理
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderStream.connect(receiptStream)
      .process(new TxPayMatchResult())
    resultStream.print()
    resultStream.getSideOutput(new OutputTag[OrderEvent]("no-recepit"))
      .print("unmatchRecepit")
    resultStream.getSideOutput(new OutputTag[ReceiptEvent]("no-pay"))
      .print("unmatchPay")
    senv.execute()
  }
}

class TxPayMatchResult() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

  //定义状态保存当前交易对应的订单支付事件和到账事件
  var payEventState: ValueState[OrderEvent] = _
  var receiptEventState: ValueState[ReceiptEvent] = _
  //定义侧输出流标签
  val noRecipit = new OutputTag[OrderEvent]("no-recepit")
  val noPay = new OutputTag[ReceiptEvent]("no-pay")

  //初始化方法
  override def open(parameters: Configuration): Unit = {
    payEventState = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay", classOf[OrderEvent]))
    receiptEventState = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))
  }

  override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //订单支付来了，判断之前是否有到账事件
    val receipt = receiptEventState.value()
    if (receipt != null) {
      //如果有，匹配账单成功，正常输出
      out.collect((pay, receipt))
      receiptEventState.clear()
      payEventState.clear()
    } else {
      //如果没有，注册定时器等待
      ctx.timerService().registerEventTimeTimer(pay.timestamp * 1000L + 5000L)
      //更新状态
      payEventState.update(pay)
    }
  }

  override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //到账信息来了，判断之前是否有订单支付事件
    val pay = payEventState.value()
    if (pay != null) {
      //如果有，正常输出
      out.collect((pay, receipt))
      payEventState.clear()
      receiptEventState.clear()
    } else {
      //如果没有，注册定时器等待
      ctx.timerService().registerEventTimeTimer(receipt.timestamp * 1000L + 3000L)
      //更新状态
      receiptEventState.update(receipt)
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //定时器触发，判断状态中哪个还存在，代表另一个没找到，输出到侧输出流
    if (payEventState.value() != null) {
      ctx.output(noRecipit, payEventState.value())
    }
    if (receiptEventState.value() != null) {
      ctx.output(noPay, receiptEventState.value())
    }
    //清空状态
    receiptEventState.clear()
    payEventState.clear()
  }
}
