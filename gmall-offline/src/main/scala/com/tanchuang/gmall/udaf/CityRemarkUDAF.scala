package com.tanchuang.gmall.udaf

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class CityRemarkUDAF extends UserDefinedAggregateFunction{

  override def inputSchema: StructType = {
    StructType(StructField("city_name",StringType)::Nil)
  }

  override def bufferSchema: StructType = {
    StructType(StructField("cityCountMap",MapType(StringType,LongType))::StructField("totalClick",LongType)::Nil)
  }

//输出类型
  override def dataType: DataType = {
    StringType
  }

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=Map[String,Long]()
    buffer(1)=0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      var cityCountMap = buffer.getMap[String, Long](0)
      val cityName = input.getString(0)

      buffer(0) = cityCountMap + (cityName ->(cityCountMap.getOrElse(cityName,0L) + 1L))
      buffer(1) = buffer.getLong(1) + 1L
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (!buffer2.isNullAt(0)){
      val map1 = buffer1.getAs[Map[String,Long]](0)
      val map2 = buffer2.getAs[Map[String,Long]](0)
      val count1 = buffer1.getAs[Long](1)
      val count2 = buffer2.getAs[Long](1)


      val resultMap: Map[String, Long] = map1.foldLeft(map2){
//        case (map2, x) => map2 + (x._1 -> (map2.getOrElse(x._1,0L)+x._2))
        case(map2,(cityName,count))=> map2 + (cityName -> (map2.getOrElse(cityName,0L)+count))
      }

      buffer1(0) = resultMap
      buffer1(1) = count1  + count2
    }
  }
//返回值
  override def evaluate(buffer: Row): Any = {
    val cityCountMap = buffer.getAs[Map[String,Long]](0)
    val totalClick = buffer.getAs[Long](1)

    val sortedCityRateList = cityCountMap.toList.sortBy(_._2)(Ordering.Long.reverse).take(2)
    var cityRateList = sortedCityRateList.map {
      case (cityName, count) => CityRate(cityName, count.toDouble / totalClick)
    }
//    cityRateList :\ CityRate("其他",cityRateList.foldLeft(1.0)(_ - _.rate))
    cityRateList :+= CityRate("其他",cityRateList.foldLeft(1.0)(_ - _.rate))
     cityRateList.mkString(",")

  }




}

case class CityRate(cityName:String,rate:Double){
  private val formatter = new DecimalFormat("0.00%")
  override def toString: String = cityName+":"+ formatter.format(rate)

}
