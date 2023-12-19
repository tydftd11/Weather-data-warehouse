package com.tongling

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DWDtoDWS {
  def main(args:Array[String]):Unit={
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    spark.sql(
      """
        |insert overwrite table dws_tl_history_weather.dws_history_weather
        |select
        |    weather_date,
        |    daytime_weather_condition,
        |    min_temperature,
        |    daytime_wind_direction,
        |    night_weather_condition,
        |    max_temperature,
        |    night_wind_direction
        |from dwd_tl_history_weather.dwd_history_weather
        |""".stripMargin)

    spark.sql(
      """
        |insert overwrite table dws_tl_history_weather.dws_history_aqi
        |select
        |    aqi_date,air_quality_grade,aqi_index,aqi_ranking_day,PM25,PM10,So2,No2,Co,O3
        |from dwd_tl_history_weather.dwd_history_aqi
        |""".stripMargin)

    spark.sql(
      """
        |insert overwrite table dws_tl_history_weather.dws_history_weather_aqi
        |select a.weather_date,a.daytime_weather_condition,a.min_temperature,
        |       a.daytime_wind_direction,a.night_weather_condition,a.max_temperature,a.night_wind_direction,
        |       b.air_quality_grade,b.aqi_index,b.aqi_ranking_day,b.PM25,b.PM10,b.So2,b.No2,b.Co,b.O3
        |from dwd_tl_history_weather.dwd_history_weather as a
        |join dwd_tl_history_weather.dwd_history_aqi as b
        |on a.weather_date = b.aqi_date
        |""".stripMargin)
    println("写入成功！！！！")
    spark.stop()
  }
}
