package com.tongling

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DWStoADS {
  def main(args:Array[String]):Unit={
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    // 铜陵市近一个月的天气情况
    spark.sql(
      """
        |insert overwrite table ads_tl_history_weather.ads_curent_month_weather_aqi
        |select weather_date,daytime_weather_condition,min_temperature,max_temperature,
        |       PM25,PM10,So2,No2,Co,O3
        |from dws_tl_history_weather.dws_history_weather_aqi
        |order by weather_date desc limit 30
        |""".stripMargin)

    // 指标2: 铜陵近三年不同天气状况的占比情况
    spark.sql(
      """
        |insert overwrite table ads_tl_history_weather.ads_weather_condition
        |select
        |     daytime_weather_condition,
        |     round(tmp1.cnt_daytime_weather_condition / tmp2.sum_daytime_weather_condition,4) as percent1
        |from (
        |    select daytime_weather_condition,
        |       count(daytime_weather_condition) as cnt_daytime_weather_condition
        |    from dws_tl_history_weather.dws_history_weather
        |    where cast(substr(weather_date,1,4) as int) > year(current_date()) -3
        |    group by daytime_weather_condition
        |) as tmp1 cross join(
        |    select count(daytime_weather_condition) as sum_daytime_weather_condition
        |    from dws_tl_history_weather.dws_history_weather
        |    where cast(substr(weather_date,1,4) as int) > year(current_date()) -3
        |) as tmp2
        |""".stripMargin)

    //指标3: 铜陵近三年不同风力风向的天数数量
    spark.sql(
      """
        |insert overwrite table ads_tl_history_weather.ads_wind_direction
        |select daytime_wind_direction,
        |       count(daytime_wind_direction) as cnt_daytime_wind_direction
        |from dws_tl_history_weather.dws_history_weather
        |where cast(substr(weather_date,1,4) as int) > year(current_date()) -3
        |group by daytime_wind_direction
        |
        |""".stripMargin)

    // 指标4: 铜陵近三年空气质量状况的天数
    spark.sql(
      """
        |insert overwrite table ads_tl_history_weather.ads_air_quality_grade
        |select air_quality_grade,
        |       count(air_quality_grade) as cnt_air_quality_grade
        |from dws_tl_history_weather.dws_history_aqi
        |where cast(substr(aqi_date,1,4) as int) > year(current_date()) -3
        |group by air_quality_grade
        |""".stripMargin)

    // 指标5: 铜陵去年PM2.5的变化情况
    spark.sql(
      """
        |insert overwrite table ads_tl_history_weather.ads_aqi_PM25
        |select tmp1.month,tmp1.avg_month_PM25,tmp2.avg_year_PM25
        |from (
        |    select substr(aqi_date,6,2) as month,round(avg(PM25),2) as avg_month_PM25
        |    from dws_tl_history_weather.dws_history_aqi
        |    where cast(substr(aqi_date,1,4) as int) = year(current_date()) - 1
        |    group by substr(aqi_date,6,2)
        |) as tmp1 cross join (
        |    select round(avg(PM25),2) as avg_year_PM25
        |    from dws_tl_history_weather.dws_history_aqi
        |    where cast(substr(aqi_date,1,4) as int) = year(current_date()) - 1
        |) as tmp2
        |""".stripMargin)

    // 指标6: 铜陵一月份平均最高气温与最低气温的变化情况
    spark.sql(
      """
        |insert overwrite table ads_tl_history_weather.ads_weather_temperature
        |select tmp1.year,tmp1.avg_max_temperature,tmp1.avg_min_temperature,tmp2.all_avg_max_temperature,tmp2.all_avg_min_temperature
        |from (
        |    select substr(weather_date,1,4) as year,round(avg(cast(regexp_replace(max_temperature,"℃",'') as int)),2) as avg_max_temperature,round(avg(cast(regexp_replace(min_temperature,"℃",'') as int)),2) as avg_min_temperature
        |    from dws_tl_history_weather.dws_history_weather
        |    where substr(weather_date,6,2) = '01'
        |    group by substr(weather_date,1,4)
        |) as tmp1 cross join (
        |    select round(avg(cast(regexp_replace(max_temperature,"℃",'') as int)),2) as all_avg_max_temperature,round(avg(cast(regexp_replace(min_temperature,"℃",'') as int)),2) as all_avg_min_temperature
        |    from dws_tl_history_weather.dws_history_weather
        |) as tmp2
        |""".stripMargin)

    spark.stop()
  }
}
