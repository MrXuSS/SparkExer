package com.haiyi.wordTest

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Mr.Xu
 * @create 2020-08-25
 *
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)

    sc.textFile("input")
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_ + _)
      .collect()
      .foreach(println)

    sc.stop()

  }

}
