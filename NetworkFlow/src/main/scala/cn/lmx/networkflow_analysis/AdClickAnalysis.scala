package cn.lmx.networkflow_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//定义输入数据的样例类
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

//定义输出数据的样例类
case class AdClickCountByProvince(windowEnd: String, province: String, count: Long)

//定义黑名单输出样例类
case class BlackListUserWarning(userid: Long, adId: Long, msg: String)

object AdClickAnalysis {
  def main(args: Array[String]): Unit = {
    // 1.环境
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置事件时间
    senv.setParallelism(1)

    // 2.读取数据
    val inputDS: DataStream[String] = senv.readTextFile("datas\\AdClickLog.csv")
    // 2.1将数据转换为指定格式
    val adLogStream: DataStream[AdClickLog] = inputDS.map(
      data => {
        val arr: Array[String] = data.split(",")
        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      }
    )
      //时间戳单调递增
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //插入异步过滤操作，将有刷单行为的用户输出到侧输出流（黑名单报警）
    val filterBlackListStream: DataStream[AdClickLog] = adLogStream
      .keyBy(
        //按照用户id和广告id分组,同一人点击同一广告数量过多则加入黑名单，不再追加数据
        data => (data.userId, data.adId)
      )
      .process(new FilterBackListUserResult(20))

    //开窗聚合
    val adCountResult: DataStream[AdClickCountByProvince] = filterBlackListStream.keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      //采用预聚合
      .aggregate(new AdCountAgg(), new AdCountWindowResult())
    adCountResult.print()
    filterBlackListStream.getSideOutput(new OutputTag[BlackListUserWarning]("warning")).print("黑名单")
    senv.execute()
  }
}

class AdCountAgg() extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdCountWindowResult() extends WindowFunction[Long, AdClickCountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdClickCountByProvince]): Unit = {
    out.collect(AdClickCountByProvince(new Timestamp(window.getEnd).toString, key, input.head))
  }
}

class FilterBackListUserResult(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
  //定义状态，保存用户对广告的点击量
  var countState: ValueState[Long] = _
  //每天0点定时清空状态的时间戳
  var reseTimerTsState: ValueState[Long] = _
  //标记当前用户是否在黑名单里
  var isBlackState: ValueState[Boolean] = _

  override def open(parameters: Configuration): Unit = {
    countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
    reseTimerTsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-ts", classOf[Long]))
    isBlackState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is_black", classOf[Boolean]))
  }

  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
    //从state中取出点击量
    val curCount: Long = countState.value()
    //判断只要是第一个数据来了，直接注册0点清空状态的定时器
    if (curCount == 0) {
      val ts = ctx.timerService().currentProcessingTime() / ((1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24) - (8 * 60 * 60 * 1000)
      reseTimerTsState.update(ctx.timerService().currentProcessingTime())
      ctx.timerService().registerProcessingTimeTimer(ts)
    }
    //判断点击量是否已经达到定义的阈值，如果超过就输出到黑名单
    if (curCount >= maxCount) {
      //判断是否已经在黑名单里，如果没有的话输出到侧输出流
      if (!isBlackState.value()) {
        isBlackState.update(true)
        //输出到侧输出流
        ctx.output(new OutputTag[BlackListUserWarning]("warning"),
          BlackListUserWarning(value.userId, value.adId, "看广告有瘾，建议送到精神病院"))
      }
      return
    }
    //正常情况
    countState.update(curCount + 1)
    out.collect(value)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    //清空状态
    if (timestamp == reseTimerTsState.value()) {
      isBlackState.clear()
      countState.clear()
      reseTimerTsState.clear()
    }
  }
}
