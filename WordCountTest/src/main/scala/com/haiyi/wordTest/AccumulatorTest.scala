package com.haiyi.wordTest

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Mr.Xu
 * @create 2020-09-01
 *
 */
object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AccumulatorTest")
    val sc: SparkContext = new SparkContext(conf)

    val inputRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4), ("a", 5)))

    val sum: LongAccumulator = sc.longAccumulator("sum")

    inputRDD.foreach{
      case (word, count)=>{
        sum.add(count)
      }
    }

    println(sum.value)
    sc.stop()
  }
}
