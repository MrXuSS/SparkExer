package com.haiyi.wordTest

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Mr.Xu
 * @create 2020-09-01
 *  利用broadcast实现join
 */
object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BroadcastTest")
    val sc: SparkContext = new SparkContext(conf)

    val inputRDD: RDD[(Int, Int)] = sc.makeRDD(List((1, 2), (2, 3)))
    val list: List[(Int, Int)] = List((1, 3), (2, 4))

    val listBroadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)

    inputRDD.map{
      case (k1, v1) =>{
        var v2:Int = 0
        for (elem <- listBroadcast.value) {
          if(elem._1 == k1){
            v2 = elem._2
          }
        }

        (k1,(v1,v2))
      }
    }.collect().foreach(println)


    sc.stop()
  }

}
