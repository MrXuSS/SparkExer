package com.haiyi.exer.sparkStreaming

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Mr.Xu
 * @create 2020-10-28 11:14
 *         读取kafka上的数据
 *         模拟出来的数据格式:
 *         时间戳,地区,城市,用户 id,广告 id
 *         1566035129449,华南,深圳,101,2
 *
 *         每天每地区每城市热门广告 Top3
 */
object RealTimeApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealTimeApp")
    val ssc = new StreamingContext(conf, Seconds(3))

    ssc.sparkContext.setCheckpointDir("D:\\Program\\WorkSpace\\IDEA_WorkSpace\\checkpoint")

    // kafka 参数
    //kafka参数声明
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

    val inputDS: DStream[AdsInfo] = dStream.map {
      case (t1, t2) => {
        val words: Array[String] = t2.split(",")
        AdsInfo(words(0).toLong, words(1), words(2), words(3), words(4))
      }
    }
//    (info) => (info, 1)
    val dayAreaCityToOne: DStream[(DayAndAreaAndCityAndAds, Long)] = inputDS.map(
      adsInfo => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val date: String = sdf.format(new Date(adsInfo.ts))
        (DayAndAreaAndCityAndAds(date, adsInfo.area, adsInfo.city, adsInfo.ads_id), 1L)
      }
    )
// (info,1 ) => (info, sum)   state
    val dayAreaCityToSum : DStream[(DayAndAreaAndCityAndAds, Long)] = dayAreaCityToOne.updateStateByKey(
      (seq: Seq[Long], buffer: Option[Long]) => {
        Option(buffer.getOrElse(0L) + seq.sum)
      }
    )
    // (day-area-city-ads,sum) => ((day,area,city),(ads,sum))
    val groupByAreaAndCityDS: DStream[((String, String,String), (String, Long))] = dayAreaCityToSum.map {
      case (daaac, sum) => {
        ((daaac.day, daaac.area,daaac.city), (daaac.ads_id, sum))
      }
    }

    val resultDS: DStream[((String,String, String), List[(String, Long)])] = groupByAreaAndCityDS.groupByKey().mapValues(
      datas => datas.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }
      ).take(3)
    )

    resultDS.print()

    ssc.start()
    ssc.awaitTermination()
  }

  case class AdsInfo(
                    ts:Long,
                    area:String,
                    city:String,
                    user_id:String,
                    ads_id:String
                    )

  case class DayAndAreaAndCityAndAds(
                                     day:String,
                                     area:String,
                                     city:String,
                                     ads_id:String
                                     )
}
