package com.haiyi.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Mr.Xu
 * @create 2020-10-28 8:22
 *  通过Streaming， 统计每3秒的单词的WordCount
 */
object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamingWordCount")
    val ssc = new StreamingContext(conf, Seconds(3))

    val inputStream: ReceiverInputDStream[String] = ssc.socketTextStream("127.0.0.1", 9999)
    val flatMapRDD: DStream[String] = inputStream.flatMap(
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
