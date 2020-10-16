package com.haiyi.wordTest

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Mr.Xu
 * @create 2020-09-01
 *
 */
object RDDFromMySql {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDFromMySql")
    val sc: SparkContext = new SparkContext(conf)

    //定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.2.201:3306/RDD"
    val userName = "root"
    val passWd = "123456"

    val rdd: JdbcRDD[(Int, String)] = new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url,userName,passWd)
      },
      "select * from student where id >= ? and id <= ?",
      1,
      3,
      3,
      result => (result.getInt(1), result.getString(2))
    )

    rdd.collect().foreach(println)

    sc.stop()
  }

}
