package com.tanchuang.gmall

import com.tanchuang.gmall.CategoryTopN.{readCondition, readTableToRDD}
import com.tanchuang.gmall.util.CategoryTop10Session
import com.tanchuang.sparkmall.common.util.bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 对于排名前 10 的品类，分别获取每个品类排名前 10 的 sessionId。
  */
object CategoryTopNSession {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("categoryTop10Category")
      .enableHiveSupport()
      .getOrCreate()

    //  表中的数据
    val rdd: RDD[UserVisitAction] = readTableToRDD(spark, "user_visit_action",
      readCondition("condition.properties", "condition.params.json"))

    //获取热度前10的品类
    val categoryList = getTop10Category(spark).map(x => x._1.toString)

    CategoryTop10Session.getCategoryTop10Session(spark, categoryList, rdd)




  }


  def getTop10Category(spark: SparkSession) = {
    CategoryTopN.categoryTop10(spark)
  }

}
