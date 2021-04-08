package cn.lmx.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

//定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryIdL: Int, behavior: String, timestamp: Long)

//定义窗口聚合结果的样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    //1.执行环境
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setParallelism(1)
    //设置事件时间
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //从文件中读取数据
    //从kafka中读取数据
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "starl:9092")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "items")
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    val inputStream: DataStream[String] = senv.addSource(
      new FlinkKafkaConsumer[String]("test5", new SimpleStringSchema(), props))
    //转换为样例类对象
    val userBehaviorDS: DataStream[UserBehavior] = inputStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      }
    )
    //添加水印时间
    val dataStream: DataStream[UserBehavior] = userBehaviorDS.assignAscendingTimestamps(_.timestamp * 1000L)
    //得到窗口聚合结果
    //过滤出点击（pv）行为
    val aggStream: DataStream[ItemViewCount] = dataStream.filter(_.behavior == "pv")
      //按照商品id分组
      .keyBy(_.itemId)
      //设置滑动窗口
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new ItemViewWindowResult())
    aggStream.keyBy(_.windowEnd).process(new TopNHotItems(3)).print()
    senv.execute()
  }
}

//自定义预聚合函数CountAgg，聚合的是商品的Count
//in:UserBehavior  ACC:商品数量累加  out：商品数量
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  //相当于声明一个累加器
  override def createAccumulator(): Long = 0L

  //每来一条数据，调用一次方法，数量加一
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  //返回结果
  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口函数
//in：输入参数类型  out：输出值类型
//key：键值类型  w：window对象
class ItemViewWindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    //组装结果返回
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

//自定义KeyedProcessFunction实现窗口中热门商品TopN排序
//key为窗口时间戳，输出结果为topN的结果字符串
class TopNHotItems(top: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  var itemState: ListState[ItemViewCount] = _

  //获取状态
  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(
      new ListStateDescriptor("itemState-state", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //每条数据都保存到状态中
    itemState.add(value)
    //注册windowEnd + 1的eventTime timer，
    //当触发时，说明收齐了属于windowEnd窗口的所有的商品数据，
    //也就是当程序看到windowEnd + 1的水位线watermark时，触发ontimer回调函数
    ctx.timerService.registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //获取收到的所有商品点击量
    import scala.collection.mutable._
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    //    val value: util.Iterator[ItemViewCount] = itemState.get().iterator()
    //    while (value.hasNext){
    //      allItems += value.next()
    //    }
    for (item <- itemState.get) {
      allItems += item
    }
    //提前清除状态中的数据，释放空间
    itemState.clear()
    //按照点击量从大到小排列
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(top)
    //将排名信息格式化成String,组装打印信息
    val result = new StringBuilder
    result.append("==================================\n")
    result.append("时间:").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedItems.indices) {
      val currentItem: ItemViewCount = sortedItems(i)
      //e.g. No1: 商品ID=12223 浏览量=2413
      result.append("No").append(i + 1).append(":")
        .append("商品ID=").append(currentItem.itemId)
        .append("浏览量=").append(currentItem.count).append("\n")
    }
    result.append("==================================\n")
    //控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
