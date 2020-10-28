package com.haiyi.test

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

/**
 * @author Mr.Xu
 * @create 2020-10-28 8:43
 *   自定义数据源
 */
object MySourceTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MySourceTest")
    val ssc = new StreamingContext(conf, Seconds(3))
    val source = new MySource("127.0.0.1", 9999)

    val inputRDD: ReceiverInputDStream[String] = ssc.receiverStream[String](source)
    inputRDD.print()
    val flatMapRDD: DStream[String] = inputRDD.flatMap(
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

  class MySource(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    override def onStart(): Unit = {
      // 启动一个新的线程来接收数据
      new Thread("Socket Receiver"){
        override def run(): Unit = {
          receive()
        }
      }.start()
    }

    override def onStop(): Unit = {

    }

    def receive() = {
      val socket = new Socket(host, port)
      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8)
      )
      var line:String = reader.readLine()
      while (!isStopped() && line != null){
        store(line)
        line = reader.readLine()
      }
      reader.close()
      socket.close()

      restart("Trying to connect again")
    }

  }

}
