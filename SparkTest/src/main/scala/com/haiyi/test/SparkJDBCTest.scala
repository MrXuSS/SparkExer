package com.haiyi.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Mr.Xu
 * @create 2020-10-21 8:50
 *
 */
object SparkJDBCTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkJDBCTest").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val jdbcDF: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.2.201:3306/SparkJDBCExer")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "student")
      .load()

    jdbcDF
        .show()

    jdbcDF.write
        .format("jdbc")
        .mode("append")
        .option("url", "jdbc:mysql://192.168.2.201:3306/SparkJDBCExer")
        .option("user", "root")
        .option("password", "123456")
        .option("dbtable", "user1")
        .save()

    spark.close()
  }
}
