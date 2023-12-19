package com.tongling

import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object MLPrediction {
  def main(args:Array[String]):Unit={
    val dataDir :String = "src/data/TL_feature_weather_AQI.txt"

    val sess: SparkSession = SparkSession.builder().appName("TL_feature_weatherAQI").master("local[2]").config("spark.testing.memory", "2147480000").getOrCreate()

    val sc: SparkContext = sess.sparkContext

    // 定义样例类
    case class Air(MEASURE:Double,AQI:Double,PM2_5:Double,
                   PM10:Double,SO2:Double,NO2:Double,
                   CO:Double,O3:Double
                  )

    // 交换
    val rd1 = sc.textFile(dataDir).map(x => x.split(",")).map(e =>
      Air(e(1).toDouble,e(2).toDouble,e(4).toDouble,e(5).toDouble,e(6).toDouble,e(7).toDouble,
        e(8).toDouble,e(9).toDouble)
    )
    // 导入隐式转换模块
    import sess.implicits._
    val predictDF: DataFrame = rd1.map(x =>
      Tuple1(Vectors.dense(x.AQI, x.PM2_5, x.PM10, x.SO2, x.NO2, x.CO, x.O3))
    ).toDF("features")

    // 加载模型
    val model = LogisticRegressionModel.load("src/data/model")

    val predicted: DataFrame = model.transform(predictDF)
      .select("features", "prediction")

    predicted
      .write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save("src/data/prediction")
  }
}
