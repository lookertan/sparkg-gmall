package com.tanchuang.realtime.app.app

import java.text.SimpleDateFormat

import com.tanchuang.realtime.app.bean.AdsInfo
import com.tanchuang.sparkmall.common.util.bean.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds}
import org.json4s.native.JsonMethods

object LastHourAdsClickApp {

  def statLastHourAdsClick(adsInfoDstream: DStream[AdsInfo]) = {
    val windowDstream = adsInfoDstream.window(Minutes(60), Seconds(5))

    val groupAdsCountDstream = windowDstream.map(adsInfo => {
      val format = new SimpleDateFormat("HH:mm")
      ((adsInfo.adsId, format.format(adsInfo.ts)), 1)
    }).reduceByKey(_ + _).map {
      case ((adsId, hourMinutes), count) => (adsId, (hourMinutes, count))
    }.groupByKey

    val jsonCountDstream = groupAdsCountDstream.map {
      case (adsId, it) => {
        import org.json4s.JsonDSL._
        val hourMinutesJson = JsonMethods.compact(JsonMethods.render(it))
        (adsId, hourMinutesJson)
      }
    }

    jsonCountDstream.foreachRDD(rdd=>{
      val result = rdd.collect()
      import collection.JavaConversions._
      val client = RedisUtil.getJedisClient
      client.hmset("last_hour_ads_click",result.toMap)
      result.foreach(println)
      client.close()

    })



  }

}
