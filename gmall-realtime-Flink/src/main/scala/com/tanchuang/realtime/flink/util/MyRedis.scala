package com.tanchuang.realtime.flink.util

import com.tanchuang.realtime.flink.bean.AdsInfo
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

class MyRedis extends RedisMapper[(AdsInfo,Int)]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"DayUserActive")
  }

  override def getKeyFromData(t: (AdsInfo, Int)): String = {
    val ads: AdsInfo = t._1
    ads.dayString + ":" + ads.userId
  }

  override def getValueFromData(t: (AdsInfo, Int)): String = {
    val ads: AdsInfo = t._1
    ads.hourString    //第一次登陆的具体时间
  }
}
