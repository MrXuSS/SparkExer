package com.haiyi.wordTest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Mr.Xu
 * @create 2020-08-25
 *
 */
object RDD_Exer {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)

    val list: List[Int] = List(3,4,1,2,5)

    val inputRDD: RDD[Int] = sc.parallelize(list, 3)

    inputRDD.map((_,1))

    sc.stop()

  }

}
