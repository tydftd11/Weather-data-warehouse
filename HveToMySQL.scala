package com.tongling

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object HveToMySQL {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val properties: Properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "123456")
    properties.put("driver", "com.mysql.jdbc.Driver")

    val JDBCURL = "jdbc:mysql://192.168.25.100:3306/tongling_weather_fenxi?useUnicode=true&characterEncoding=UTF-8"

    // 拿到ads层的数据
    val df1: DataFrame = spark.sql("select * from ads_tl_history_weather.ads_curent_month_weather_aqi")
    val df2: DataFrame = spark.sql("select * from ads_tl_history_weather.ads_weather_condition")
    val df3: DataFrame = spark.sql("select * from ads_tl_history_weather.ads_wind_direction")
    val df4: DataFrame = spark.sql("select * from ads_tl_history_weather.ads_air_quality_grade")
    val df5: DataFrame = spark.sql("select * from ads_tl_history_weather.ads_aqi_PM25")
    val df6: DataFrame = spark.sql("select * from ads_tl_history_weather.ads_weather_temperature")

    // 写入mysql
    df1.write.mode("overwrite").jdbc(JDBCURL, "curent_month_weather_aqi", properties)
    df2.write.mode("overwrite").jdbc(JDBCURL, "weather_condition", properties)
    df3.write.mode("overwrite").jdbc(JDBCURL, "wind_direction", properties)
    df4.write.mode("overwrite").jdbc(JDBCURL, "air_quality_grade", properties)
    df5.write.mode("overwrite").jdbc(JDBCURL, "aqi_PM25", properties)
    df6.write.mode("overwrite").jdbc(JDBCURL, "weather_temperature", properties)

    println("写入成功")
    spark.stop()
  }
}
