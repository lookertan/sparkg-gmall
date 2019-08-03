package com.tanchuang.realtime.app.app

import com.tanchuang.realtime.app.bean.AdsInfo
import com.tanchuang.sparkmall.common.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object RealTimeApp {
  /*
  测试
   */
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val conf = new SparkConf().setMaster("local[2]").setAppName("realtimeApp")
    val ssc: StreamingContext = new StreamingContext(conf, Milliseconds(500))
    val topic = "ads_log0225"
    val inputDstream = MyKafkaUtil.getDStream(ssc, topic)

    //对流的转换 //1563596390366,华北,北京,101,5
    val adsInfoDstream: DStream[AdsInfo] = inputDstream.map(record => {
      val splits = record.value().split(",")
      AdsInfo(splits(0).toLong, splits(1), splits(2), splits(3), splits(4))
    })
    adsInfoDstream.print()
    //过滤黑名单中的用户点击行为
    //    val fielteredDstream: DStream[AdsInfo] = BlackListUserCount.filterUserToBlackList(ssc, adsInfoDstream)

    //    需求5： 实现实时的动态黑名单机制：将每天对某个广告点击超过 100 次的用户拉入黑名单。
    //    BlackListUserCount.checkUserToBlackList(fielteredDstream)


    //    需求6：每天各地区各城市各广告的点击流量实时统计
    //    val dayAreaCityAdsCountDstream: DStream[(String, Int)] = statDayAreaCityAdClickCount(ssc,adsInfoDstream)


    /*
        需求7. 每天每地区热门广告 Top3
        来源数据是是上个需求得到的结果: 每天每地区每城市每广告点击量
     */
    //    getAreaAdsTop3Click(dayAreaCityAdsCountDstream)

    //需求8: ：各广告最近 1 小时内各分钟的点击量  窗口大小1h  滑动步长5s
//    statLastHourAdsClick(adsInfoDstream)

    ssc.start()
    ssc.awaitTermination()

  }
}
