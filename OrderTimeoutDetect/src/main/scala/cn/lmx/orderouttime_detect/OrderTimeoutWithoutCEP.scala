package cn.lmx.orderouttime_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

object OrderTimeoutWithoutCEP {
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
    //自定义processFunction进行复杂事件的检测
    val resultStream: DataStream[OrderResult] = orderStream.keyBy(_.orderId)
      .process(new OrderPayMatchResult())
    //输出正常结果
    resultStream.print()
    //输出侧道输出
    resultStream.getSideOutput(new OutputTag[OrderResult]("orderTimeout")).print("timeout=>")
    senv.execute()
  }
}

class OrderPayMatchResult() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

  var isCreateState: ValueState[Boolean] = _
  var isPayState: ValueState[Boolean] = _
  var TimerTsState: ValueState[Long] = _
  //定义侧道输出标签
  val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

  override def open(parameters: Configuration): Unit = {
    isCreateState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-created", classOf[Boolean]))
    isPayState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-pay", classOf[Boolean]))
    TimerTsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))
  }

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    //获取状态
    val isCreate: Boolean = isCreateState.value()
    val isPayed: Boolean = isPayState.value()
    val timerTs: Long = TimerTsState.value()
    //判断当前事件类型
    //1.来的是create，要判断是否pay过
    if (value.eventType == "create") {
      //1.1如果已经支付(正常支付)，实处匹配成功的结果
      if (isPayed) {
        out.collect(OrderResult(value.orderId, "payed successfully"))
        //已经处理完毕，清空状态和定时器
        isCreateState.clear()
        isPayState.clear()
        TimerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      } else {
        //1.2没有支付，注册一个十五分钟的定时器
        val ts: Long = value.timestamp * 1000L + 900 * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        //更新状态
        TimerTsState.update(ts)
        isCreateState.update(true)
      }
    } else {
      //2.来的是pay，要判断是否createguo
      if (isCreate) {
        //2.1有create,判断pay的时间是否超时
        if (value.timestamp * 1000L < timerTs) {
          //没有超时，正常输出
          out.collect(OrderResult(value.orderId, "payed successfully"))
        } else {
          //超时，报警，测到输出
          ctx.output(orderTimeoutOutputTag, OrderResult(value.orderId, "palyed but alreade timeout"))
        }
        //只要输出结果，当前订单处理完毕，清空状态和定时器
        isCreateState.clear()
        isPayState.clear()
        TimerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      } else {
        //2.2,没有create，注册定时器处理，等到pay的时间就可以
        ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L)
        //更新状态
        TimerTsState.update(value.timestamp * 100L)
        isPayState.update(true)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    //定时器操作
    //1.pay数据，没有create
    if (isPayState.value()) {
      ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "no created buy payed"))
    } else {
      //2.create数据，没有pay
      ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
    }
    //清空状态
    isCreateState.clear()
    isPayState.clear()
    TimerTsState.clear()
  }
}
