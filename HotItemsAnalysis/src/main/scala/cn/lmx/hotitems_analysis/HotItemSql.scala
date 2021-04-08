package cn.lmx.hotitems_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object HotItemSql {
  def main(args: Array[String]): Unit = {

    //执行环境
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setParallelism(1)
    //设置事件时间
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //从文件中读取数据
    val inputStream: DataStream[String] = senv.readTextFile("datas\\UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = inputStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      }
    )
      //添加水印时间
      .assignAscendingTimestamps(_.timestamp * 1000L)
    //定义表环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tabEnv: StreamTableEnvironment = StreamTableEnvironment.create(senv, settings)
    //基于DataStream创建表
    val dataTable: Table = tabEnv.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)
    //使用TableApi进行开窗聚合操作
    //过滤点击（pv）操作
    val aggTable: Table = dataTable.filter('behavior === "pv")
      //设置窗口，窗口为一小时，每五分钟一个窗口
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
      .groupBy('itemId, 'sw)
      //查询商品id，窗口结束时间，商品浏览次数
      .select('itemId, 'sw.end as 'windowEnd, 'itemId.count as 'cnt)
    //使用SQL实现TopN操作
    //创建一个临时表存储数据和查询
    tabEnv.createTemporaryView("aggtable", aggTable, 'itemId, 'windowEnd, 'cnt)
    //执行查询
    val resultTable: Table = tabEnv.sqlQuery(
      """
        |select
        | itemId,windowEnd,cnt,row_num
        |from
        | (select
        |   itemId,windowEnd,cnt,
        |   row_number() over(partition by windowEnd order by cnt desc) as row_num
        | from aggtable
        | )
        |where row_num <= 5
        |""".stripMargin
    )
    resultTable.toRetractStream[Row].print()
    senv.execute()
  }
}
