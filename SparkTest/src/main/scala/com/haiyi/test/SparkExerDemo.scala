package com.haiyi.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Mr.Xu
 * @create 2020-08-31
 *  练习： 计算每个省份广告点击量的TopN
 *  数据模型 ：1516609143867 6 7 64 16
 *            时间戳  省份  城市  用户  广告
 */
object SparkExerDemo {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkExerDemo")
    val sc: SparkContext = new SparkContext(conf)

    // 1.读取文件
    val inputRDD: RDD[String] = sc.textFile("WordCountTest/src/main/resources/agent.log")

    // 2.对数据进行转型 （省份-广告，1）
    val mapRDD: RDD[(String, Int)] = inputRDD.map(line => {
      val words: Array[String] = line.split(" ")
      (words(1) + "_" + words(4), 1)
    })

    // 3.聚合，求得每个省份每个广告的和 （省份-广告，sum）
    val adsAndSumRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    // 4. 转型： （省份-广告，sum）=> (省份，（广告，sum）)
    // map 和case一起使用，模式匹配；
    val adsAndCitySumRDD: RDD[(String, (String, Int))] = adsAndSumRDD.map {
      // (ads, sum) 会报错；推断错误
      case (ads, sum) => {
        val words: Array[String] = ads.split("_")
        (words(0), (words(1), sum))
      }
    }

    // 5.对数据进行分组
    val resultRDD: RDD[(String, Iterable[(String, Int)])] = adsAndCitySumRDD.groupByKey()

    // 6.对同组的数据进行排序取前三
    val finalRDD: RDD[(String, List[(String, Int)])] = resultRDD.mapValues(iter => {
      iter.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }
      ).take(3)
    })

    finalRDD.collect().foreach(println)

    sc.stop()


  }

}
