package com.haiyi.exer.sparkcore

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

/**
 * Top10品类的Top10Session
 *
 * @author Mr.Xu
 * @create 2020-10-16 14:58
 *         热门品类top10
 *         2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_37_2019-07-17 00:00:02_手机_-1_-1_null_null_null_null_3
 *         2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_48_2019-07-17 00:00:10_null_16_98_null_null_null_null_19
 *         2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_6_2019-07-17 00:00:17_null_19_85_null_null_null_null_7
 */
object Top10Category_v3_top10Session {
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

    val acc = new CategoryAcc
    sc.register(acc, "CategoryAcc")

    userVisitAction.foreach(
      action =>{
        acc.add(action)
      }
    )

    /**
     * ((鞋，click), 10)
     * ((鞋，order), 20)
     * ((鞋，pay), 10)
     * ((衣服，click),10)
     *  ====>
     *  (鞋, (((鞋，click), 10), ((鞋，order), 20)))
     */
    val resultMap: mutable.Map[(String, String), Long] = acc.value
    val infoes: Map[String, mutable.Map[(String, String), Long]] = resultMap.groupBy(_._1._1)
    val finalInfoes: immutable.Iterable[CategoryCountInfo] = infoes.map {
      case (id, map) => {
        CategoryCountInfo(id,
          map.getOrElse((id, "click"), 0L),
          map.getOrElse((id, "order"), 0L),
          map.getOrElse((id, "pay"), 0L))
      }
    }
    finalInfoes.toList.sortWith(
      (left, right) => {
        if(left.clickCount > right.clickCount) {
          true
        }else if (left.clickCount == right.clickCount){
          if(left.orderCount > right.orderCount){
            true
          } else if(left.orderCount == right.orderCount){
            left.payCount > right.payCount
          } else{
            false
          }
        } else{
          false
        }
      }
    ).take(10).foreach(println)

    val list: List[String] = finalInfoes.map(_.categoryId).toList
    val broadcastIds: Broadcast[List[String]] = sc.broadcast(list)

    val filterRDD: RDD[UserVisitAction] = userVisitAction.filter(
      action => {
        if(action.click_category_id != -1) {
          broadcastIds.value.contains(action.click_category_id.toString)
        }else{
          false
        }
      }
    )
    val acc1 = new CategoryAndSessionAcc
    sc.register(acc1, "CategoryAndSessionAcc")

    filterRDD.foreach(
      action => acc1.add(action)
    )

    val accMap: mutable.Map[(String, String), Long] = acc1.value
    val groupMap: Map[String, mutable.Map[(String, String), Long]] = accMap.groupBy(_._1._1)
    val valueListMap: Map[String, List[((String, String), Long)]] = groupMap.mapValues(
      map => {
        map.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        )
      }.take(10)
    )
    valueListMap.values.foreach(println)

    sc.stop()
  }

  // (category_id-session_id, sum)
  class CategoryAndSessionAcc extends AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] {

    private var map: mutable.Map[(String, String), Long] = mutable.Map[(String, String), Long]()

    override def isZero: Boolean ={
      map.isEmpty
    }

    override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = {
      new CategoryAndSessionAcc
    }

    override def reset(): Unit = {
      map.clear()
    }

    override def add(v: UserVisitAction): Unit = {
      val key = (v.click_category_id.toString, v.session_id)
      map.put(key, map.getOrElse(key, 0L) + 1)
    }

    override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
      var map1 = map
      var map2 = other.value
      map = map1.foldLeft(map2)(
        (innerMap, otherMap) =>{
          innerMap.put(otherMap._1, innerMap.getOrElse(otherMap._1, 0L) + otherMap._2)
          innerMap
        }
      )
    }

    override def value: mutable.Map[(String, String), Long] = map
  }

  /**
   * ((鞋，click), 1)   => ((鞋，click), 10)
   * ((鞋，order), 1)
   * ((鞋，pay), 1)
   * ((衣服，click),1)
   */
  class CategoryAcc extends AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] {

    private var map: mutable.Map[(String, String), Long] = mutable.Map[(String, String), Long]()

    override def isZero: Boolean = {
      map.isEmpty
    }

    override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = {
      new CategoryAcc
    }

    override def reset(): Unit = {
      map.clear()
    }

    override def add(v: UserVisitAction): Unit = {
      if(v.click_category_id != -1) {
        val key: (String, String) = (v.click_category_id.toString, "click")
        map.put(key,map.getOrElse(key, 0L) + 1L)
      }else if(v.order_category_ids != "null") {
        val words: Array[String] = v.order_category_ids.split(",")
        words.foreach(
          id => {
            val key: (String, String) = (id, "order")
            map.put(key, map.getOrElse(key, 0L) + 1)
          }
        )
      }else if(v.pay_category_ids != "null") {
        val words: Array[String] = v.pay_category_ids.split(",")
        words.foreach(
          id => {
            val key: (String, String) = (id, "pay")
            map.put(key, map.getOrElse(key, 0L) + 1)
          }
        )
      }
    }

    override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
      val map1 = map
      val map2: mutable.Map[(String, String), Long] = other.value
      map = map1.foldLeft(map2)(
        (innerMap, otherMap) =>{
          innerMap.put(otherMap._1, innerMap.getOrElse(otherMap._1, 0L) + otherMap._2)
          innerMap
        }
      )
    }

    override def value: mutable.Map[(String, String), Long] = map
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
