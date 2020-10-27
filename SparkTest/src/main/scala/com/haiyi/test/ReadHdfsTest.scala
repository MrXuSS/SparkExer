package com.haiyi.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * @author Mr.Xu
 * @create 2020-10-21 9:07
 *
 */
object ReadHdfsTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "xu")
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReadHdfsTest")
    val sc = new SparkContext(conf)

    val inputRDD: RDD[String] = sc.textFile("hdfs://192.168.2.201:9000/hdfsTest/test1.txt")
    inputRDD.collect().foreach(println)

    inputRDD.saveAsTextFile("hdfs://192.168.2.201:9000/hdfsTest/test")

    sc.stop()
  }

}
