package cn.lmx.networkflow_analysis


import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Map

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//定义输入数据的样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

//定义窗口聚合结果的样例类
case class PageViewCount(url: String, windowEnd: Long, count: Long)

object HotPageAnalysis {
  def main(args: Array[String]): Unit = {
    //执行环境
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setParallelism(1)
    //设置事件时间
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //从文件中读取数据
    //    val inputStream: DataStream[String] = senv.readTextFile("datas\\apache.log")
    val inputStream: DataStream[String] = senv.socketTextStream("starl", 9999)
    //装换成样例类，并提取时间戳，添加水印
    val dataStream: DataStream[ApacheLogEvent] = inputStream.map(
      data => {
        val arr: Array[String] = data.split("\\s+")
        //对时间格式进行转换 得到时间戳
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val ts: Long = sdf.parse(arr(3)).getTime
        ApacheLogEvent(arr(0), arr(2), ts, arr(5), arr(6))
      }
    ).assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(10)) {
        //提取事件时间
        override def extractTimestamp(logTime: ApacheLogEvent): Long = logTime.eventTime
      }
    )
      //简单数据清洗，取出不符合格式的url
      //进行开窗聚合，以及排序输出
      .filter(data => {
        val pattern = "^((?!\\.(css|js|png)$).)*$".r
        (pattern findFirstIn data.url).nonEmpty
      })
    val aggDS: DataStream[PageViewCount] = dataStream.filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      //允许延迟时间+侧道输出
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
      .aggregate(new PageCounAgg(), new PageViewCountWindowResult())
    //排序操作
    //获取侧道输出的迟到数据
    aggDS.getSideOutput(new OutputTag[ApacheLogEvent]("late")).print()
    //按照窗口分组，并排序出topN
    dataStream.print("data=>")
    aggDS.print("agg=>")
    aggDS.keyBy(_.windowEnd).process(new TopNPage(5)).print()
    senv.execute()
  }
}

//输入数据进行聚合操作
class PageCounAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//输入聚合结果，拼装输出
class PageViewCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, window.getEnd, input.head))
  }
}

class TopNPage(num: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {
  //  var pageState: ListState[PageViewCount] = _
  var pageState: MapState[String, Long] = _

  override def open(parameters: Configuration): Unit = {
    //通过getRuntimeContext.getListState获取状态
    //    pageState = getRuntimeContext.getListState(
    //      new ListStateDescriptor[PageViewCount]("pageViewCount", classOf[PageViewCount]))
    pageState = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageViewCount", classOf[String], classOf[Long]))
  }

  override def processElement(value: PageViewCount, context: KeyedProcessFunction[Long, PageViewCount, String]#Context, collector: Collector[String]): Unit = {
    //把每一条数据放入pageState
    //    pageState.add(value)
    pageState.put(value.url, value.count)
    //注册一个定时器，每个windowEnd+1时进行排序
    //    context.timerService().registerEventTimeTimer(value.windowEnd + 1)
    context.timerService().registerEventTimeTimer(context.getCurrentKey + 1)
    //再注册一个定时器，在一分钟之后触发，这时窗口已经彻底关闭，不再聚合结果，可以清空状态
    context.timerService().registerEventTimeTimer(context.getCurrentKey + 60000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //判断定时器触发时间，如果是窗口结束时间1分钟后，那么可以直接清除状态
    if (timestamp == ctx.getCurrentKey + 60000L) {
      pageState.clear()
      return
    }
    //从状态中取出数据
    //    val it: util.Iterator[PageViewCount] = pageState.get().iterator()
    import scala.collection.mutable.ListBuffer
    var allPageViewCounts: ListBuffer[(String, Long)] = new ListBuffer()
    //    while (it.hasNext) {
    //      allPageViewCounts += it.next()
    //    }

    val it: util.Iterator[Map.Entry[String, Long]] = pageState.entries().iterator()
    while (it.hasNext) {
      val entry = it.next()
      allPageViewCounts += ((entry.getKey, entry.getValue))
    }

    //清空状态
    //    pageState.clear()
    //排序 按照访问量去TopN
    val sortedPageViewCounts: ListBuffer[(String, Long)] = allPageViewCounts.sortWith(_._2 > _._2).take(num)
    //    allPageViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(num)
    //将结果组装字符串
    val result = new StringBuffer
    result.append("==================================\n")
    result.append("时间:").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedPageViewCounts.indices) {
      val currentItem: (String, Long) = sortedPageViewCounts(i)
      //e.g. No1: 商品ID=12223 浏览量=2413
      result.append("No").append(i + 1).append(":")
        .append("URL=").append(currentItem._1).append("\t")
        .append("热度=").append(currentItem._2).append("\n")
    }
    result.append("==================================\n")
    //控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
