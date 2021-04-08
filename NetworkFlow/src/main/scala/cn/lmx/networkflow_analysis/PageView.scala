package cn.lmx.networkflow_analysis

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

//定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryIdL: Int, behavior: String, timestamp: Long)

//定义输出数据的样例类
case class PvCount(windowEnd: Long, count: Long)

object PageView {
  /**
   * 网站总浏览量（PV）的统计
   * 实时统计每小时内的网站 PV
   **/
  def main(args: Array[String]): Unit = {
    // 1.环境
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置事件时间
    //    senv.setParallelism(1)

    // 2.读取数据
    val path: String = "datas\\UserBehavior.csv"
    val inputDS: DataStream[String] = senv.readTextFile(path)
    // 2.1将数据转换为指定格式
    import org.apache.flink.api.scala._
    val assStream: DataStream[UserBehavior] = inputDS.map(
      data => {
        val arr: Array[String] = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      }
    )
      // 2.2抽取事件时间(数据源时间为秒，需要转换为毫秒级的时间戳)
      .assignAscendingTimestamps(_.timestamp * 1000L)
    assStream
      // 2.3处理数据
      .filter(_.behavior == "pv")
      .map(x => ("pv", 1))
      //      .map(new MyMapper()) //自定义分组，解决数据倾斜
      .keyBy(_._1)
      // 滚动窗口
      .timeWindow(Time.hours(1))
      .aggregate(new PvCountAgg(), new PvCountWindowResult())
      .rebalance.print()
    //    pvStream.keyBy(_.windowEnd)
    //      .process(new TotalPvCountResult()).print()
    senv.execute()
  }
}

class MyMapper() extends MapFunction[UserBehavior, (String, Int)] {
  override def map(value: UserBehavior): (String, Int) = {
    (Random.nextString(10), 1)
  }
}

class PvCountAgg() extends AggregateFunction[(String, Int), Long, Long] {
  //创建累加器并赋初始值
  override def createAccumulator(): Long = 0L

  //聚合的核心逻辑
  override def add(in: (String, Int), acc: Long): Long = acc + 1

  //最后返回结果
  override def getResult(acc: Long): Long = acc

  //在分布式的情况下，需要从各个节点merge聚合结果
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class PvCountWindowResult() extends WindowFunction[Long, PvCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect(PvCount(window.getEnd, input.head))
  }
}

class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCount, PvCount] {

  var totalPvState: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    totalPvState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-pv", classOf[Long]))
  }

  override def processElement(value: PvCount, context: KeyedProcessFunction[Long, PvCount, PvCount]#Context, collector: Collector[PvCount]): Unit = {
    //每进来一个数据，将count值叠加在当前状态上
    val currentCount: Long = totalPvState.value()
    totalPvState.update(currentCount + value.count)
    //注册一个定时器，窗口关闭时（windowEnd+1）触发
    context.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
    val totalPvCount: Long = totalPvState.value()
    out.collect(PvCount(timestamp, totalPvCount))
  }
}