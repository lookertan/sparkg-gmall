package com.tanchuang.gmall.util

import java.util.{Properties, UUID}

import com.tanchuang.gmall.bean.CategorySession
import com.tanchuang.sparkmall.common.util.ConfigurationUtil
import com.tanchuang.sparkmall.common.util.bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object CategoryTop10Session {

  def getCategoryTop10Session(spark: SparkSession, categoryList: List[String], rdd: RDD[UserVisitAction]) = {
    //过滤不是top10category 的action
    val filteredUserVisitActionRDD = rdd.filter(usa => {
      if (usa.click_category_id != -1) {
        categoryList.contains(usa.click_category_id.toString)
      } else if (usa.order_category_ids != null) {
        val arr = usa.order_category_ids.split(",")
        categoryList.intersect(arr).size > 0
      } else if (usa.pay_category_ids != null) {
        val arr = usa.pay_category_ids.split(",")
        categoryList.intersect(arr).size > 0
      } else {
        false
      }
    })
    //    println(filteredUserVisitActionRDD.count())
    //将RDD[userVisitAction] 转换成RDD[((cid,sid),1)]
    val categorySessionOne = filteredUserVisitActionRDD.flatMap(usa => {
      if (usa.click_category_id != -1) {
        Array(((usa.click_category_id + "", usa.session_id), 1))
      } else if (usa.order_category_ids != null) {
        val categoryArr = usa.order_category_ids.split(",")
        categoryArr.intersect(categoryList).map(cid => ((cid + "", usa.session_id), 1))
      } else {
        val categoryArr = usa.pay_category_ids.split(",")
        categoryArr.intersect(categoryList).map(cid => ((cid + "", usa.session_id), 1))
      }
    })
    //将RDD[((cid,sid),1)] 转换成 拍好序的 RDD[(String, Iterable[(String, Int)])]
    val categorySeesionCountList: RDD[(String, Iterable[(String, Int)])] = categorySessionOne.reduceByKey(_ + _).map {
      case ((cid, sid), count) => (cid, (sid, count))
    }.groupByKey()
    val sortedCategorySessionCount: RDD[(String, List[(String, Int)])] = categorySeesionCountList.map {
      case (cid, sidCountIte) => {
        (cid, sidCountIte.toList.sortBy(_._2)(Ordering.Int.reverse).take(10))
      }
    }

    val taskId = UUID.randomUUID().toString
    //转换成样例类
    val flatCategorySesssionCount = sortedCategorySessionCount.flatMap {
      case (cid, sidCountList) => sidCountList.map {
        case (sid, count) => CategorySession(taskId, cid, sid, count)
      }
    }
    import spark.implicits._
    val configs = ConfigurationUtil("config.properties")
    val url = configs.getString("jdbc.url")
    val tableName = "category_top10_session_count"

    val props = new Properties()
    props.setProperty("user", configs.getString("jdbc.user"))
    props.setProperty("password", configs.getString("jdbc.password"))

    flatCategorySesssionCount.toDF.write.mode(SaveMode.Append).jdbc(url, tableName, props)


  }


  def zipUtilsArray[T](ite1:Iterable[T])={
    val sourceList = ite1.toList
    val list1 = sourceList.slice(0,ite1.size-1)
    val list2 = sourceList.slice(1,ite1.size)
    list1.zip(list2)
  }
}
