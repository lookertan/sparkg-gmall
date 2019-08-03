package com.tanchuang.gmall

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.tanchuang.gmall.acc.MyAcc
import com.tanchuang.gmall.bean.Condition
import com.tanchuang.sparkmall.common.util.ConfigurationUtil
import com.tanchuang.sparkmall.common.util.bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/*
  每个品类的 点击、下单、支付 的量来统计热门品类.
  1.统计每个品类的点击数，下单数，支付数
  2.根据点击数，下单数，支付数 排序 取前10
 */
object CategoryTopN {


  def main(args: Array[String]): Unit = {

    //1.读取user_visit_action 的信息
    val spark = SparkSession.builder().master("local[2]").appName("CategoryTopN")
      .enableHiveSupport().getOrCreate()

    val list = categoryTop10(spark)

    list.foreach(println)
  }

  def readTableToRDD(spark: SparkSession, tableName: String, condition: Condition) = {
    import spark.implicits._
    spark.sql("use sparkmall")

    var str =
      s"""
         |select
         | t.*
         | from $tableName t join user_info u on t.user_id=u.user_id
         | where 1=1
         |
      """.stripMargin;
    if (isNotEmpty(condition.startDate)) {
      str += s" and date >= '${condition.startDate}'"
    }
    if (isNotEmpty(condition.endDate)) {
      str += s" and date <= '${condition.endDate}'"
    }
    if (condition.startAge > 0) {
      str += s" and u.age >= ${condition.startAge}"
    }
    if (condition.endAge > 0) {
      str += s" and u.age <= ${condition.endAge}"
    }

    val ds = spark.sql(str).as[UserVisitAction]
    ds.rdd
  }

  def readCondition(conf: String, key: String) = {
    val str = ConfigurationUtil("conditions.properties").getString("condition.params.json")
    JSON.parseObject(str, classOf[Condition])
  }

  def categoryTop10(spark:SparkSession) = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")

    var props: Properties = new Properties()
    props.setProperty("user", ConfigurationUtil("config.properties").getString("jdbc.user"))
    props.setProperty("password", ConfigurationUtil("config.properties").getString("jdbc.password"))

    val acc = new MyAcc()
    spark.sparkContext.register(acc)

    val rdd: RDD[UserVisitAction] = readTableToRDD(spark, "user_visit_action",
      readCondition("condition.properties", "condition.params.json"))
    rdd.foreach(x => acc.add(x))


    //    acc.value.toList.sortBy(x=>(-x._2._1,-x._2._2,-x._2._3)).take(10).foreach(println)
    val tuples = acc.value.toList.sortBy(x => (-x._2._1, -x._2._2, -x._2._3)).take(10)

    val tuples2 = tuples.map(x => (x._1.toLong, x._2._1, x._2._2, x._2._3))


    import spark.implicits._
    val df = spark.sparkContext.makeRDD(tuples2).toDF("categoryId", "clickCount", "orderCount", "payCount")
    //SaveMode.Overwrite  覆写以前的记录
    df.write.mode(SaveMode.Overwrite).jdbc(ConfigurationUtil("config.properties").getString("jdbc.url"), "category_top10", props)

    tuples2

  }

}
