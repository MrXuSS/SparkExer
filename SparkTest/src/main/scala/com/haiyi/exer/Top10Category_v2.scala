package com.haiyi.exer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * 优化， 采用reduceByKey代替groupByKey
 * @author Mr.Xu
 * @create 2020-10-16 14:58
 *         热门品类top10
 *         2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_37_2019-07-17 00:00:02_手机_-1_-1_null_null_null_null_3
 *         2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_48_2019-07-17 00:00:10_null_16_98_null_null_null_null_19
 *         2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_6_2019-07-17 00:00:17_null_19_85_null_null_null_null_7
 */
object Top10Category_v2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Top10Category")
    val sc = new SparkContext(conf)

    // 获取原始数据
    val inputRDD = sc.textFile("SparkTest/src/main/resources/user_visit_action.txt")

    // line => UserVisitAction()
    val userVisitAction = inputRDD.map(
      line => {
        val words = line.split("_")
        UserVisitAction(
          words(0),
          words(1).toLong,
          words(2),
          words(3).toLong,
          words(4),
          words(5),
          words(6).toLong,
          words(7).toLong,
          words(8),
          words(9),
          words(10),
          words(11),
          words(12).toLong
        )
      }
    )

    // UserVisitAction => CategoryCountInfo(id, 1, 0, 0), CategoryCountInfo(id, 0, 1, 0)
    val flatMapRDD: RDD[(String, CategoryCountInfo)] = userVisitAction.flatMap(
      action => {
        if (action.click_category_id != -1) {
          List((action.click_category_id.toString, CategoryCountInfo(action.click_category_id.toString, 1, 0, 0)))
        } else if (action.order_category_ids != "null") {
          val listBuffer = new ListBuffer[(String, CategoryCountInfo)]
          val ids = action.order_category_ids.split(",")
          for (id <- ids) {
            listBuffer.append((id, CategoryCountInfo(id, 0, 1, 0)))
          }
          listBuffer
        } else if (action.pay_category_ids != "null") {
          val listBuffer = new ListBuffer[(String, CategoryCountInfo)]
          val ids = action.pay_category_ids.split(",")
          for (id <- ids) {
            listBuffer.append((id, CategoryCountInfo(id, 0, 0, 1)))
          }
          listBuffer
        } else {
          Nil
        }
      }
    )
    val reduByKeyRDD: RDD[(String, CategoryCountInfo)] = flatMapRDD.reduceByKey(
      (info1, info2) => {
        info1.clickCount = info1.clickCount + info2.clickCount
        info1.orderCount = info1.orderCount + info2.orderCount
        info1.payCount = info1.payCount + info2.payCount
        info1
      }
    )

    // 排序
    val finalResult: Array[CategoryCountInfo] = reduByKeyRDD
      .map(_._2)
      .sortBy(data => (data.clickCount, data.orderCount, data.payCount), false)
      .take(10)

    finalResult.foreach(println)

    sc.stop()
  }
  /**
   * 用户访问动作表
   *
   * @param date               用户点击行为的日期
   * @param user_id            用户的ID
   * @param session_id         Session的ID
   * @param page_id            某个页面的ID
   * @param action_time        动作的时间点
   * @param search_keyword     用户搜索的关键词
   * @param click_category_id  某一个商品品类的ID
   * @param click_product_id   某一个商品的ID
   * @param order_category_ids 一次订单中所有品类的ID集合
   * @param order_product_ids  一次订单中所有商品的ID集合
   * @param pay_category_ids   一次支付中所有品类的ID集合
   * @param pay_product_ids    一次支付中所有商品的ID集合
   * @param city_id            城市 id
   */
  case class UserVisitAction(date: String,
                             user_id: Long,
                             session_id: String,
                             page_id: Long,
                             action_time: String,
                             search_keyword: String,
                             click_category_id: Long,
                             click_product_id: Long,
                             order_category_ids: String,
                             order_product_ids: String,
                             pay_category_ids: String,
                             pay_product_ids: String,
                             city_id: Long)
  case class CategoryCountInfo(categoryId: String,
                               var clickCount: Long,
                               var orderCount: Long,
                               var payCount: Long)
}
