package com.haiyi.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext, Time}

/**
 * @author Mr.Xu
 * @create 2020-10-28 10:06
 *
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WindowTest")
    val ssc = new StreamingContext(conf, Seconds(2))

    val inputDS: ReceiverInputDStream[String] = ssc.socketTextStream("127.0.0.1", 9999)
    // 窗口长度为6s， 滑动为4s
//    val winDS: DStream[String] = inputDS.window(Seconds(6), Seconds(4))
//     滚动窗口
    val winDS: DStream[String] = inputDS.window(Seconds(4))

    val flatMapRDD: DStream[String] = winDS.flatMap(
      line => {
        val words: Array[String] = line.split(" ")
        words
      }
    )
    val mapRDD: DStream[(String, Int)] = flatMapRDD.map((_, 1))
    val resultRDD: DStream[(String, Int)] = mapRDD.reduceByKey(_ + _)
    resultRDD.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
