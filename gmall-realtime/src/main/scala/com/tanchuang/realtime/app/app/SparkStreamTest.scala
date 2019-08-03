package com.tanchuang.realtime.app.app

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("yarn").setAppName("wc")

    val ssc = new StreamingContext(conf,Seconds(2))
    import org.apache.spark.streaming.dstream._
    val stream: ReceiverInputDStream[String] = ssc.rawSocketStream("hadoop102",7777)
    stream.print()




    ssc.start()
    ssc.awaitTermination()

  }

}
