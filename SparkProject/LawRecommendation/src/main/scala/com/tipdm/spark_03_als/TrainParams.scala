package com.tipdm.spark_03_als

import com.tipdm.common.CommonObject
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

/**
  * 02 参数寻优
  */
object TrainParams {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark = CommonObject.getHead("TrainParams")

    val modeData = spark.read.table("law.law_model")
    modeData.show(10,false)
    //数据切分==>切分训练集 和 测试集
    //训练集 做相关的训练
    val Array(train,test) = modeData.randomSplit(Array(0.8,0.2))
    println("train数据量："+train.count(),"test数据量："+test.count())

    //设置模型参数 -- 协同过滤---最小二乘法
    val als = new ALS()
      //必选参数
      .setItemCol("UrlEncoding") //网址转换后编码
      .setUserCol("userIdEncoding")
      .setRatingCol("label")

    val paraGrid = new ParamGridBuilder()
      .addGrid(als.alpha,Array(0.3,1.0,3.0)) //权重alpha
      .addGrid(als.maxIter,Array(8,10,12)) //最大迭代次数
      .addGrid(als.rank,Array(8,10,12)) //rank值
      .addGrid(als.implicitPrefs,Array(true,false)) //隐式反馈
      .addGrid(als.regParam,Array(0.1,0.09,0.3)) //正则化参数
      .build()
    //交叉验证
    val cv = new CrossValidator()
      .setEstimator(als)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paraGrid)
      .setNumFolds(2)
    val model = cv.fit(train)
    val pre = model.transform(test)
    print(pre)

    spark.stop()
  }

}
