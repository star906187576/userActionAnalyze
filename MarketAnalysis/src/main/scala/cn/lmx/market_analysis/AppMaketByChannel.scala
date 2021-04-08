package cn.lmx.market_analysis

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

//定义输入数据的样例类
case class MaketUserBehvior(userId: String, behvior: String, channel: String, timestamp: Long)

//定义输出数据样例类
case class MarketViewCount(windowStart: String, windowEnd: String, channel: String, behvior: String, count: Long)

//自定义数据源生成数据
class CustomerSource() extends RichSourceFunction[MaketUserBehvior] {
  var isRunning = true
  val behaviorTypes = Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")
  val channerSet = Seq("AppStore", "XiaomiStore", "HuaweiStore", "weibo", "wechat", "tieba")

  override def run(ctx: SourceFunction.SourceContext[MaketUserBehvior]): Unit = {
    var num = 0L
    val maxCOUNT: Long = Long.MaxValue

    while (isRunning && num < maxCOUNT) {
      val userId = UUID.randomUUID().toString
      ctx.collect(MaketUserBehvior(userId, behaviorTypes(Random.nextInt(behaviorTypes.size)), channerSet(Random.nextInt(channerSet.size)), System.currentTimeMillis()))
      num += 1
      TimeUnit.MILLISECONDS.sleep(1500)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}

/**
 * 根据渠道进行分组聚合，自定义数据源
 */
object AppMaketByChannel {
  def main(args: Array[String]): Unit = {
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //自定义数据源
    val sourceStream: DataStream[MaketUserBehvior] = senv.addSource(new CustomerSource)
    sourceStream.print("data=>")
    //抽取事件时间
    sourceStream.assignAscendingTimestamps(_.timestamp)
      //开窗统计输出
      .filter(_.behvior != "uninstall")
      //      .keyBy(_.channel)
      //      .keyBy(_.behvior)
      .keyBy(data => (data.channel, data.behvior))
      .timeWindow(Time.days(1), Time.seconds(5))
      //process全量聚合
      .process(new MarketCountByChannel())
      .print()
    senv.execute()
  }
}

class MarketCountByChannel() extends ProcessWindowFunction[MaketUserBehvior, MarketViewCount, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[MaketUserBehvior], out: Collector[MarketViewCount]): Unit = {
    val start = new Timestamp(context.window.getStart).toString
    val end = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behvior: String = key._2
    val count = elements.size
    out.collect(MarketViewCount(start, end, channel, behvior, count))
  }
}

