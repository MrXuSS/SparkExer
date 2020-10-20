package com.haiyi.exer

import com.haiyi.exer.Top10Category_v2_top10Session.UserVisitAction
import com.sun.xml.internal.bind.v2.TODO
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Mr.Xu
 * @create 2020-10-20 10:25
 *  计算页面单跳转换率， 可指定计算哪些页面
 */


object PageFlow {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("PageFlow").setMaster("local[*]")
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

    // TODO 设定要计算哪些页面的单跳转换率    ((1,2),(2,3),(3,4) ...)
    val list = List(1, 2, 3, 4, 5, 6, 7)
    val idZipList: List[(Int, Int)] = list.init.zip(list.tail)

    // TODO 1.计算分母, 直接求各页面的和， 直接过滤掉不需要的page_id
    val pageIdToOneRDD: RDD[(Long, Long)] = userVisitAction.filter(
      action => {
        list.init.contains(action.page_id)
      }
    ).map(
      action => (action.page_id, 1L)
    )

    // TODO （pageid， 1） => (pageId, sum)
    val pageIdsToSum: RDD[(Long, Long)] = pageIdToOneRDD.reduceByKey(_ + _)
    val map: Map[Long, Long] = pageIdsToSum.collect().toMap

    // TODO 2.计算分子
    // TODO 根据session分组
    val groupByRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitAction.groupBy(_.session_id)
    val groupSortMapRDD: RDD[(String, List[(String, Long)])] = groupByRDD.mapValues(
      datas => {

        // TODO 按照时间排序， 由于服务器可能搭载负载均衡器， 导致数据的时间顺序混乱
        val actions: List[UserVisitAction] = datas.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )
        val actionList = actions.map(
          action => {
            action.page_id
          }
        )

        // TODO 同SESSION内的pageid做zip， 就是页面的单跳  （1,2,3...） => ((1,2),(2,3)...)
        val pageFlows = actionList.zip(actionList.tail)
        pageFlows
          .filter(
          pages => {
            idZipList.contains(pages)
          }
        ).map {
             // TODO (page1_page2, 1)
          case (t1, t2) => {
            (t1 + "_" + t2, 1L)
          }
        }
      }
    )

    val flatMapRDD: RDD[(String, Long)] = groupSortMapRDD.map(_._2).flatMap(list => list)

    // TODO (page1_page2, 1)=> TODO (page1_page2, sum)
    val reduceRDD: RDD[(String, Long)] = flatMapRDD.reduceByKey(_ + _)

    // TODO 计算
    reduceRDD.foreach{
      case (pageid, sum) => {
        val words: Array[String] = pageid.split("_")
        val pageSum: Long = map.getOrElse(words(0).toLong, 1L)
        println(pageid +"="+ sum / pageSum.toDouble)
      }
    }


    sc.stop()
  }
}
