package com.tanchuang.realtime.app.app

import com.tanchuang.realtime.app.bean.AdsInfo
import com.tanchuang.sparkmall.common.util.bean.RedisUtil
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListUserCount {
  val redishost = "hadoop102"
  val redisport = 6379
  val countKey = "user:day:adsClick" //redis 里面key  将每个用户的点击数记下
  val blackListKey = "blackList" //超过点击次数的用户写入blackList
  //将每天对某个广告点击超过 100 次的用户拉入黑名单。
  //  redis: key -> "user:day:adsClick"   field -> "day:userId:ads"
  def checkUserToBlackList(adsInfoDstream: DStream[AdsInfo]) = {
    //对流中的每条消息进行处理
    adsInfoDstream.foreachRDD(adsInfos => {
      adsInfos.foreachPartition(adsInfoItr => {
        val client = RedisUtil.getJedisClient

        adsInfoItr.foreach(adsInfo => {
          var oldUserAdsClickCount: Long = 0L
          println(adsInfo)

          try {
            oldUserAdsClickCount = client.hget(countKey, s"${adsInfo.dayString}:${adsInfo.userId}:${adsInfo.adsId}").toLong
          } catch {
            case e =>
          }
          //          如果该用户点击次数小于100 则加 超过100 就不统计了
          if (oldUserAdsClickCount.toLong < 100) {
            client.hincrBy(countKey, s"${adsInfo.dayString}:${adsInfo.userId}:${adsInfo.adsId}", 1)
          } else {
            client.sadd(blackListKey, adsInfo.userId)
          }

        })
        client.close()
      })
    })
  }

    def filterUserToBlackList(ssc:StreamingContext,adsInfoDstream: DStream[AdsInfo]) = {

      val fielteredAdsInfoDstream = adsInfoDstream.transform(adsInfos => {
        val client: Jedis = RedisUtil.getJedisClient
        val blackList = client.smembers(blackListKey)
        val blacklistBC = ssc.sparkContext.broadcast(blackList)

        val fielteredAdsInfoRDD = adsInfos.filter(adsInfo => !blacklistBC.value.contains(adsInfo.userId))

        fielteredAdsInfoRDD
      })
      fielteredAdsInfoDstream
      }


}
