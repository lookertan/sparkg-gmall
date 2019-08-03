package com.tanchuang.realtime.flink.bean

import java.text.SimpleDateFormat
import java.util.Date


case class AdsInfo(ts: Long, area: String, city: String, userId: String, adsId: String) {
  val dayString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
  val hourString: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(ts))
  override def toString: String = s"$ts:$dayString:$area:$city:  $userId  :$adsId"
}
