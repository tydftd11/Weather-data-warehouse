package com.tongling

import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

object TrainML {
  def main(args:Array[String]):Unit={
    val dataDir :String = "src/data/TL_history_weatherAQI .txt"

    val sess: SparkSession = SparkSession.builder().appName("TL_histroy_weatherAQI").master("local[2]").config("spark.testing.memory", "2147480000").getOrCreate()

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
    val trainDF: DataFrame = rd1.map(x => (
      x.MEASURE, Vectors.dense(x.AQI, x.PM2_5, x.PM10, x.SO2, x.NO2, x.CO, x.O3)
    )).toDF("label", "features")

//    trainDF.show()

    // 创建逻辑回归对象
    val lr: LogisticRegression = new LogisticRegression()

    // 迭代次数
    lr.setMaxIter(100)

    // 创建模型
    val model: LogisticRegressionModel = lr.fit(trainDF)

    // 测试数据
    val testDF: DataFrame = sess.createDataFrame(Seq((2.0, Vectors.dense(55,  29, 61, 15, 49, 0.93, 26)),
      (2.0, Vectors.dense(60,  39, 70, 12, 49, 1.08, 28)),
      (2.0, Vectors.dense(94,  70, 115, 12, 67, 1.18, 16))
    )).toDF("label", "features")

    // 保存模型
    model.write.overwrite().save("src/data/model")
    val tested: DataFrame = model.transform(testDF).select("features", "label","prediction")

    tested.show()
  }
}
