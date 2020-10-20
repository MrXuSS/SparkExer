package com.haiyi.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn}

/**
 * @author Mr.Xu
 * @create 2020-10-20 15:17
 *
 */
object SparkSqlTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSqlTest")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    val df: DataFrame = spark.read.json(ClassLoader.getSystemResource("user.json").getPath)

    df.show()

    df.filter($"age" > 19).show

    df.createOrReplaceTempView("user")
    spark.sql("select * from user").show()

    spark.udf.register("toUpper", (str:String) => {
      str.toUpperCase
    })

    spark.sql("select toUpper('xiaoming')").show()

    spark.udf.register("MyAvg", new MyAvg())
    spark.sql("select MyAvg(age) from user").show()

    val avgAgeUDAFClass = new AvgAgeUDAFClass
    val column: TypedColumn[UserX, Double] = avgAgeUDAFClass.toColumn
    val ds: Dataset[UserX] = df.as[UserX]
    ds.select(column).show()

    spark.stop()
  }


  class MyAvg extends UserDefinedAggregateFunction{
    override def inputSchema: StructType = {
      StructType(Array(StructField("age", LongType)))
    }

    override def bufferSchema: StructType = {
      StructType(Array(StructField("sum", LongType), StructField("count", LongType)))
    }

    override def dataType: DataType = {
      DoubleType
    }

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1L
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0).toDouble / buffer.getLong(1)
    }
  }

  case class UserX(id:Long, name:String, age:Long )
  case class AvgBuffer( var sum:Long, var count:Long )
  // 自定义聚合函数 （强类型）0
  // 1. 继承Aggregator
  // 2. 重写 方法
  class AvgAgeUDAFClass extends Aggregator[UserX, AvgBuffer, Double]{
    override def zero: AvgBuffer = {
      AvgBuffer(0L,0L)
    }

    override def reduce(buff: AvgBuffer, in: UserX): AvgBuffer = {
      buff.sum = buff.sum + in.age
      buff.count = buff.count + 1L
      buff
    }

    override def merge(buff1: AvgBuffer, buff2: AvgBuffer): AvgBuffer = {
      buff1.sum = buff1.sum + buff2.sum
      buff1.count = buff1.count + buff2.count
      buff1
    }

    override def finish(buff: AvgBuffer): Double = {
      buff.sum.toDouble / buff.count
    }

    override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }
}

