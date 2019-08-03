package com.tanchuang.gmall.util

import java.util.Properties

import com.tanchuang.gmall.udaf.CityRemarkUDAF
import com.tanchuang.sparkmall.common.util.ConfigurationUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

object AreaProductTop3 {
  /*
  连接hive仓库 使用sql获取数据
 */
  def getAreaProductTop3(spark: SparkSession, taskId: String) = {

    spark.sql("use sparkmall")
    spark.udf.register("remark",new CityRemarkUDAF)
    val df1 = spark.sql(
      """
        |select
        | c.*,
        | p.product_name ,
        | u.click_product_id
        |from user_visit_action u join city_info c join product_info p on u.city_id=c.city_id  and p.product_id=u.click_product_id
        |where u.click_category_id != -1
      """.stripMargin)
    df1.createOrReplaceTempView("t1")

    spark.sql(
      """
        |select
        |  area,
        |  product_name,
        |  count(*) click_count,
        |  remark(city_name) city_remark
        | from t1
        | group by area,product_name
      """.stripMargin).createOrReplaceTempView("t2")

    spark.sql(
      """
        |select
        | area,
        | product_name,
        | click_count,
        | city_remark,
        | row_number() over(partition by area order by click_count desc) rowNum
        |from t2
      """.stripMargin).createOrReplaceTempView("t3")

    val resultDf = spark.sql(
"""
  |select
  | area,
  | product_name,
  | click_count,
  | city_remark
  | from t3
  | where rowNum <=3
""".stripMargin)

//    resultDf.show(1000)
    val configuration = ConfigurationUtil("config.properties")
    val props = new Properties()
    props.setProperty("user",configuration.getString("jdbc.user"))
    props.setProperty("password",configuration.getString("jdbc.password"))

    resultDf.write.mode(SaveMode.Append).jdbc(configuration.getString("jdbc.url"),"area_click_top3",props)


  }

}
