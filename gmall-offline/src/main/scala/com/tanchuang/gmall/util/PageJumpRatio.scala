package com.tanchuang.gmall.util

import java.text.DecimalFormat

import com.tanchuang.gmall.bean.Condition
import com.tanchuang.sparkmall.common.util.JDBCUtil
import com.tanchuang.sparkmall.common.util.bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PageJumpRatio {
//  第一种先过滤
  def getPageJumpRatio(spark: SparkSession, rdd: RDD[UserVisitAction], condition: Condition, taskId: String) = {

    val pageflow = condition.targetPageFlow.split(",")

    val tuples = CategoryTop10Session.zipUtilsArray[String](pageflow)
    val pageFlowList = tuples.map {
      case (p1, p2) => p1 + "->" + p2
    }

    val filterUVARDD = rdd.filter(uva=> pageflow.contains(uva.page_id+""))
    val pageClickCountMap = filterUVARDD.map(uva=>(uva.page_id,1)).countByKey()
    pageClickCountMap.foreach(println)

    val pageJumpFlowRDD = filterUVARDD.groupBy(uva => uva.session_id).flatMap {
      case (seesionId, uvaIte) => {
        val sortedActionList = uvaIte.toList.sortBy(_.action_time).map(_.page_id)
        val pageJumpList = CategoryTop10Session.zipUtilsArray(sortedActionList)
        pageJumpList.map { case (p1, p2) => p1 + "->" + p2 }
      }
    }
    val jumpFlowCountMap = pageJumpFlowRDD.filter(jumpflow=>pageFlowList.contains(jumpflow)).map(flow=>(flow,1)).countByKey()

    val formatter = new DecimalFormat("0.00%")
    val jumpFlowCountRate = jumpFlowCountMap.map {
      case (jumpFlow, count) => {
        val sourcePage = jumpFlow.split("->")(0).toLong
        (jumpFlow, formatter.format(count.toDouble/pageClickCountMap.get(sourcePage).get))
      }
    }
    jumpFlowCountRate.toList.foreach(println)

    //写入指定表中
    val sql="insert into page_conversion_rate values(?,?,?)"
    val argsList = jumpFlowCountRate.map {
      case (flow, count) => Array(taskId, flow, count)
    }

    JDBCUtil.executeBatchUpdate(sql,argsList)

  }

}
