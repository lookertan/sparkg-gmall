package com.tanchuang.gmall.app

import java.util.UUID

import com.tanchuang.gmall.CategoryTopN
import com.tanchuang.gmall.bean.Condition
import com.tanchuang.gmall.util.AreaProductTop3
import com.tanchuang.sparkmall.common.util.bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object OfflineApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]")
      .appName("offline")
      .enableHiveSupport()
      .getOrCreate()
    //限制条件
    val condition: Condition = CategoryTopN.readCondition("conditions.properties", "condition.params.json")
    //原数据
    val rdd: RDD[UserVisitAction] = CategoryTopN.readTableToRDD(spark, "user_visit_action", condition)

    val taskId = UUID.randomUUID().toString
    //计算页面跳转率
//    PageJumpRatio.getPageJumpRatio(spark, rdd, condition, taskId)
   AreaProductTop3.getAreaProductTop3(spark,taskId)



  }


}
