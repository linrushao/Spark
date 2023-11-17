package com.tipdm.spark_03_als

import com.tipdm.common.CommonObject
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 03模型的构建和保存以及模型均方误差
  */
object ModelPingFen {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark: SparkSession = CommonObject.getHead("ModelPingFen")
    val modeData: DataFrame = spark.read.table("law.law_model")

    //数据切分==>切分训练集 和 测试集
    //训练集 做相关的训练
    val Array(train,test) = modeData.randomSplit(Array(0.8,0.2))
    println("train数据量："+train.count(),"test数据量："+test.count())

    //设置模型参数 -- 协同过滤---最小二乘法
    val als = new ALS()
      //必选参数
      .setItemCol("UrlEncoding")
      .setUserCol("userIdEncoding")
      .setRatingCol("label")
      //可选参数
      .setRank(10)
      .setAlpha(1.0)
      .setMaxIter(8)
      .setImplicitPrefs(false)
      .setRegParam(0.09)

    val model: ALSModel = als.fit(train)
    //将模型里面的NaN数据删除， //关闭冷启动（防止计算误差不产生NaN）
    model.setColdStartStrategy("drop")
    val pre: DataFrame = model.transform(test)
    pre.show()
    //模型的保存
    //model.save("hdfs://master:8020/model/ALS")
    //模型的调用
    //PipelineModel.load("")
    val evaluator = new RegressionEvaluator()
          .setMetricName("rmse")
          .setLabelCol("label")
          .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(pre)
    println(s"均方误差=$rmse")


    //为用户推荐前10名的电影
    val userRecs = model.recommendForAllUsers(10)
    userRecs.show(10)
    //为网址推荐前10的用户
    val UrlReces = model.recommendForAllItems(10)
    UrlReces.show(10)
    //未指定用户组生成10个网址推荐

    val users = modeData.select(als.getUserCol).distinct().limit(3)
    val userSubsetResc = model.recommendForUserSubset(users,10)
    userSubsetResc.show(10)
    //指定网址生成10个用户推荐
    val urls = modeData.select(als.getItemCol).distinct().limit(3)
    val urlSubsetResc = model.recommendForItemSubset(urls,10)
    urlSubsetResc.show(10)
    spark.stop()

  }

}
