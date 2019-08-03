package com.tanchuang.realtime.app.app

import com.tanchuang.sparkmall.common.util.bean.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods

//需求7: 每天每地区热门广告 Top3
object AreaAdsTop3Click {

  val redisKeyprefix = "area:ads:top3"

  def getAreaAdsTop3Click(dayAreaCityAdsCountDstream: DStream[(String, Int)]) = {

    val dayAreaAdsCountDstream: DStream[(String, Iterable[(String, (String, Int))])] = dayAreaCityAdsCountDstream.map {
      case (dayAreaCityAds, click) =>
        val Array(day, area, _, ads) = dayAreaCityAds.split(":")
        //过滤掉城市
        ((day, area, ads), click)
    }.reduceByKey(_ + _) //将不同城市的数据进行合并
      .map {
      case ((day, area, ads), click) => (day, (area, (ads, click)))
    }.groupByKey()

    val dayMapCountDstream: DStream[(String, Map[String, Iterable[(String, (String, Int))]])] = dayAreaAdsCountDstream.map {
      case (day, itr) =>
        (day, itr.groupBy(_._1))
    }
    //去除掉 Iterable中的area
    val dayMapAreaAdsCountList: DStream[(String, Map[String, List[(String, Int)]])] = dayMapCountDstream.map {
      case (day, map) => {
        val map1 = map.map {
          case (area, itr) => {
            val newItr = itr.map {
              case (area, (ads, count)) => (ads, count)
            }
            (area, newItr.toList.sortBy(-_._2).take(3))
          }
        }
        (day, map1)
      }
    }
//    将数据写出
    dayMapAreaAdsCountList.foreachRDD(rdd => {
      rdd.foreachPartition(itr => {
        val client = RedisUtil.getJedisClient
        itr.foreach {
          case (day, map) => {
            map.foreach {
              case (area, list) =>
                //用fastjson转换
                import org.json4s.JsonDSL._
                val str = JsonMethods.compact(JsonMethods.render(list))
                client.hset(redisKeyprefix+":"+day, area, str)
//                println(s"$day:$area:$str")  调试
            }
          }
        }
      })
    })


  }

}


/*
结果为:            daystring,Map(area,List(ads,click))
思路分析:
1.原Dstream数据为:    ("2019-07-21:华南:广州:4",12)   map
2."2019-07-21:华南:广州:4".split(":")=> ((daystring,area,ads),click) reduceBykey(_+_) =>  ((daystring,area,ads),clickCount)
3.((daystring,area,ads),clickCount)  map => (daystring,(area,ads,clickCount)) groupBykey=>(daystring, Interable(area,ads,click))
4.daystring Iterable((area,ads,click))map groupby (area) =>daystring  Map(area,Iterable((area,ads,click))) => map Iterable((area,ads,click)).toList sortBy(-click).take(3)


 */