package com.tongling

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object MySQLConnect {
  private val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")

  // 开启元数据支持
  private val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

  // 配置相关的mysql的连接配置
  // 配置jdbc
  final val JDBCURL = "jdbc:mysql://192.168.25.100:3306/tongling_weather?useUnicode=true&characterEncoding=UTF-8"

  // 配置
  def getDF(tablename: String) = {
    val properties: Properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "123456")
    properties.put("driver", "com.mysql.jdbc.Driver")
    val df: DataFrame = spark.read.jdbc(JDBCURL, tablename, properties)
    df
  }
}
