package com.tanchuang.realtime.flink.app

import java.util.Properties

import com.tanchuang.realtime.flink.bean.AdsInfo
import com.tanchuang.realtime.flink.util.MyRedis
import com.tanchuang.sparkmall.common.util.{ConfigurationUtil, MyKafkaUtil}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig


/**
 * 用flink完成spark的实时项目
 * 需求
 *     1.实时统计 dau 每日客户数
 */
object ReadTimeApp {

  def main(args: Array[String]): Unit = {
    //获取flink stream执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //以事件时间
    //    env.setParallelism(1)
    val topic = ConfigurationUtil.propsReader("config.properties").getProperty("kafka.realtime.topic")
    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build()
    val props = new Properties()
    for (entry <- MyKafkaUtil.kafkaParam.toIterator) {
      props.setProperty(entry._1, entry._2.toString)
    }
    //从kafka已经获取到源数据
    val kfDstream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), props))
    //需求1. 计算日活用户， 去重写入redis
    kfDstream.map(msg => {
      val Array(ts, area, city, userId, adsId): Array[String] = msg.split(",")
      AdsInfo(ts.trim.toLong, area.trim, city.trim, userId.trim, adsId.trim)
    }
    )
      .assignAscendingTimestamps(_.ts)
      .map((_, 1))
      .keyBy(x=> x._1.dayString +":" +x._1.userId) //每一天为一组
      .sum(1)
      .filter(_._2 == 1)
      .addSink(new RedisSink[(AdsInfo, Int)](conf, new MyRedis))
    //已经写入到redis中


    env.execute()


  }

}
