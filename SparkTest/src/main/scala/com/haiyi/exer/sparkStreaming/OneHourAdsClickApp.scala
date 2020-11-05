package com.haiyi.exer.sparkStreaming

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
 * @author Mr.Xu
 * @create 2020-11-05 14:41
 *  每十秒统计过去一分钟的每个广告点击量
 */
object OneHourAdsClickApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OneHourAdsClickApp")
    val ssc = new StreamingContext(conf, Seconds(5))

    val brokers = "node1:9092"
    val topic = "ads_log"
    val group = "bigdata-1"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      "zookeeper.connect" -> "node1:2181",
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    val dStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(topic))

    val adsId: DStream[((String, String), Int)] = dStream.map {
      case (t1, t2) => {
        val words: Array[String] = t2.split(",")
        val sdf = new SimpleDateFormat("mm:ss")
        val date: String = sdf.format(new Date(words(0).toLong))
        ((words(4), date.init+"0"), 1)
      }
    }

    val windowDS: DStream[((String, String), Int)] = adsId.window(Minutes(1), Seconds(10))
    val resultDS: DStream[((String, String), Int)] = windowDS.reduceByKey(_ + _)
    resultDS.print()


    ssc.start()
    ssc.awaitTermination()
  }

}