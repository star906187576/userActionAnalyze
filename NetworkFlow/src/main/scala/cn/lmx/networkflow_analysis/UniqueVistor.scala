package cn.lmx.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//定义输出数据的样例类 统计每小时内的网站UV
case class UvCount(windowEnd: Long, count: Long)

//用户
object UniqueVistor {
  def main(args: Array[String]): Unit = {
    // 1.环境
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置事件时间
    //    senv.setParallelism(1)

    // 2.读取数据
    val inputDS: DataStream[String] = senv.readTextFile("datas\\UserBehavior.csv")
    // 2.1将数据转换为指定格式
    import org.apache.flink.api.scala._
    val dataStream: DataStream[UserBehavior] = inputDS.map(
      data => {
        val arr: Array[String] = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      }
    )
      // 2.2抽取事件时间(数据源时间为秒，需要转换为毫秒级的时间戳)
      .assignAscendingTimestamps(_.timestamp * 1000L)
    //筛选行为为pv的操作
    dataStream.filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1)) //不分组，基于dataStream开一个小时的滚动窗口
      .apply(new UvCountResult())
      .print()
    senv.execute()
  }
}

class UvCountResult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, values: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //定义一个set
    var userIdSet: Set[Long] = Set()
    //遍历所有数据 将userId添加到set中，自动去重
    val value: Iterator[UserBehavior] = values.iterator
    while (value.hasNext) userIdSet += value.next().userId
    //将set的size作为去重后的UV的数量输出
    out.collect(UvCount(window.getEnd, userIdSet.size))
  }
}
