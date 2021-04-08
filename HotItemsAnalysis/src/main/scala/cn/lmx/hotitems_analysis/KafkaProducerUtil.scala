package cn.lmx.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.{BufferedSource, Source}


/**
 * 生产数据到kafka
 */
object KafkaProducerUtil {
  def main(args: Array[String]): Unit = {
    //设置配置信息
    val props = new Properties()
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "starl:9092,starl01:9092,starl02:9092")
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //生产者
    val producer = new KafkaProducer[String, String](props)
    //从文件中读取到kafka
    val source: BufferedSource = Source.fromFile("datas\\UserBehavior.csv")
    for (line <- source.getLines()) {
      //封装kafka记录对象
      val record = new ProducerRecord[String, String]("test5", line)
      producer.send(record)
    }
    producer.close()
  }
}
