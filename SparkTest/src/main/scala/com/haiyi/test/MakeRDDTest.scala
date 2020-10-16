package com.haiyi.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Mr.Xu
 * @create 2020-08-26
 *
 */

object MakeRDDTest {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MakeRDDTest")
    val sc: SparkContext = new SparkContext(conf)

    val list: List[Int] = List(1, 2, 3, 4, 5)

    val inputRDD: RDD[Int] = sc.parallelize(list)
//    val inputRDD: RDD[Int] = sc.makeRDD(list,2)
//    val inputRDD: RDD[String] = sc.textFile("input",3)

//    inputRDD.mapPartitions(datas=>{
//      datas.map(_ * 2)
//    }).collect().foreach(println)

//    inputRDD.mapPartitionsWithIndex(
//      (index, datas)=>{
//        datas.map( data =>(index, data) )
//    }).collect().foreach(println)


    sc.stop()

  }

}
