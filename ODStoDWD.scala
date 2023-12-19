package com.tongling

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ODStoDWD {
  def main(args:Array[String]):Unit={
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

   // 脏数据过滤，使数据规范化
   // 将历史数据拆开
   // dwd_history_weather
   spark.sql(
     """
       |insert into table dwd_tl_history_weather.dwd_history_weather
       |select
       |    substr(regexp_replace(weather_date,"[\\u4e00-\\u9fa5]",'-'),1,10) as weather_date,
       |    split(weather_condition,"/")[0] as daytime_weather_condition,
       |    split(min_max_temperature,"/")[0] as min_temperature,
       |    split(wind_direction,"/")[0] as daytime_wind_direction,
       |    split(weather_condition,"/")[1] as night_weather_condition,
       |    split(min_max_temperature,"/")[1] as max_temperature,
       |    split(wind_direction,"/")[1] as night_wind_direction
       |from ods_tl_history_weather.ods_history_weather
       |""".stripMargin)

    spark.sql(
      """
        |insert into table dwd_tl_history_weather.dwd_history_aqi
        |select  aqi_date,air_quality_grade,aqi_index,aqi_ranking_day,PM25,PM10,So2,No2,Co,O3
        |from ods_tl_history_weather.ods_history_aqi
        |""".stripMargin)

    println("写入成功!!!")
    spark.stop()

  }
}
