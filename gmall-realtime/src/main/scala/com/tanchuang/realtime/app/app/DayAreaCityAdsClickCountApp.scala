package com.tanchuang.realtime.app.app

import com.tanchuang.realtime.app.bean.AdsInfo
import com.tanchuang.sparkmall.common.util.bean.RedisUtil._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

object DayAreaCityAdsClickCountApp {


  /*
      key ->  "date:area:city:ads"
    kv -> field:   2018-11-26:华北:北京:5     value: 12001
    rdd[AdsInfo] => rdd[date:area:city:ads, 1] => reduceByKey(_+_)
    => updaeStateByKey(...)
    设置 checkpoint    //我觉得也可以用 hincrby(key,1)
    需求6：每天各地区各城市各广告的点击流量实时统计
   */
  def statDayAreaCityAdClickCount(ssc: StreamingContext, fielteredDstream: DStream[AdsInfo]) = {
    ssc.sparkContext.setCheckpointDir("hdfs://hadoop103:9000/sparkmall0225/checkpoint")
    val dayAreaCityAdsClickCountKey = "date:area:city:ads"
    val fieldCountDstream = fielteredDstream.transform(adsInfoRDD => {
      adsInfoRDD.map(adsInfo => (s"${adsInfo.dayString}:${adsInfo.area}:${adsInfo.city}:${adsInfo.adsId}", 1)).reduceByKey(_ + _)
    })
    val cateFieldDstream = fieldCountDstream.updateStateByKey((seq: Seq[Int], option: Option[Int]) => Some(seq.sum + option.getOrElse(0)))

    cateFieldDstream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val jedis = getJedisClient
/*        iter.foreach {
          case (field, count) => jedis.hset(dayAreaCityAdsClickCountKey, field, count.toString)
        }    */
        iter.foreach (
          x => jedis.hset(dayAreaCityAdsClickCountKey, x._1, x._2.toString)
        )
      })
    })

    cateFieldDstream
  }

}
