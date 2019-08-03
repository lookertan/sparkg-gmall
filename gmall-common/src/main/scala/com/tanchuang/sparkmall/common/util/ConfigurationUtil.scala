package com.tanchuang.sparkmall.common.util

import java.io.InputStreamReader
import java.util.Properties

import org.apache.commons.configuration2.{FileBasedConfiguration, PropertiesConfiguration}
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters

import scala.collection.mutable

/**
  * 用来读取配置文件的工具类
  */
object ConfigurationUtil {
  // 这个 map 用来存储配置文件名和在这个文件内定义的配置
  private val configs = mutable.Map[String, FileBasedConfiguration]()

  // 参数是配置文件名
  def apply(propertiesFileName: String) = {
    // 根据配置文件名来获取来获取对应的配置.
    // 如果 map 中存在这一的配置文件, 则读取配置文件的内容并更新到 map 中
    configs.getOrElseUpdate(
      propertiesFileName,
      new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration]).configure(new Parameters().properties().setFileName(propertiesFileName)).getConfiguration
    )
  }

  def propsReader(propertiesFileName: String) = {
//    val file = new File(propertiesFileName)
//    val reader = new InputStreamReader(new FileInputStream(propertiesFileName))
    val name = this.getClass.getClassLoader.getResourceAsStream(propertiesFileName)
//    val name = ClassLoader.getSystemResourceAsStream()
    val reader = new InputStreamReader(name)
    val props = new Properties()
    props.load(reader)
    props
    //
  }

  def main(args: Array[String]): Unit = {
    var reader: Properties = propsReader("config.properties")
    println(reader.getProperty("kafka.broker.list"))

//    val config = ConfigurationUtil("config.properties")
    //  private val reader: Properties = ConfigurationUtil.propsReader("D:\\workspace\\sparkg-gmall\\gmall-common\\src\\main\\resources\\config.properties")
    // val broker_list = reader.getProperty("kafka.broker.list")
/*    val broker_list = config.getString("kafka.broker.list")
    println(broker_list)*/
  }
}
