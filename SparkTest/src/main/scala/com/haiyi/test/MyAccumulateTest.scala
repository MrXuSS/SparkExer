package com.haiyi.test


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * @author Mr.Xu
 * @create 2020-10-16 13:42
 *  自定义累加器
 */
object MyAccumulateTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AccumulatorTest")
    val sc: SparkContext = new SparkContext(conf)

    val inputRDD: RDD[String] = sc.makeRDD(List("hello", "hi","hello", "world", "hi"))

    val myAccumulate = new MyAccumulate
    // 注册累加器
    sc.register(myAccumulate, "MyAccumulate")

    inputRDD.foreach(
      str => {
        myAccumulate.add(str)
      }
    )

    println(myAccumulate.value)

    sc.stop()
  }
  class MyAccumulate extends AccumulatorV2[String, mutable.Map[String, Int]]{

    private var resultMap = mutable.Map[String, Int]()

    // 是否为空
    override def isZero: Boolean = {
      resultMap.isEmpty
    }

    // 复制
    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
      new MyAccumulate
    }

    // 重置
    override def reset(): Unit = {
      resultMap.clear()
    }

    // 添加
    override def add(v: String): Unit = {
      resultMap(v) = resultMap.getOrElse(v, 0) + 1
    }

    // 合并
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      resultMap = resultMap.foldLeft(other.value)(
        (innerMap, kv) => {
          innerMap(kv._1) =  innerMap.getOrElse(kv._1, 0) + kv._2
          innerMap
        }
      )
    }

    // 获取value
    override def value: mutable.Map[String, Int] = resultMap
  }
}

