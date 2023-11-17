package com.linrushao

import org.apache.spark.SparkContext
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.functions.{dense_rank, max}

/**
 * @Author linrushao
 * @Date 2023-08-25
 */
object ModelRecommendation {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("ModelRecommendation")
      .getOrCreate()

    //创建SparkContext
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //加载原始数据为DataFrame
    val mealDF: DataFrame = spark.read
      .json("hdfs://master:8020/data/MealRatings_201705_201706.json")
    // 使用时间戳最大值来获取最新评分记录
    val newMealDF: DataFrame = mealDF.groupBy("MealID", "UserID")
      .agg(max("ReviewTime").alias("ReviewTime"))
      .join(mealDF, Seq("MealID", "UserID", "ReviewTime"))

    // 对用户ID和菜品ID进行去重和排序，并生成编码
    val distinctUsers: Dataset[Row] = newMealDF.select("UserID").distinct()
    val userEncodings: DataFrame = distinctUsers
      .withColumn("UserEncoded", dense_rank().over(orderBy("UserID")))

    val distinctMeals: Dataset[Row] = newMealDF.select("MealID").distinct()
    val mealEncodings: DataFrame = distinctMeals
      .withColumn("MealEncoded", dense_rank().over(orderBy("MealID")))

    // 将编码后的用户ID和菜品ID与原始数据进行连接，替换原始数据中的ID列
    val dataWithUserEncoding: DataFrame = newMealDF.join(userEncodings, Seq("UserID"), "left")
    val dataWithMealEncoding: DataFrame = dataWithUserEncoding.join(mealEncodings, Seq("MealID"), "left")
    //  dataWithMealEncoding.show(10)
    // 根据 ReviewTimestamp 排序数据
    val sortedData: Dataset[Row] = dataWithMealEncoding.orderBy("ReviewTime")

    // 计算数据集的总记录数
    val totalRecords: Long = sortedData.count()

    // 计算训练集、验证集、测试集的分割点
    val trainEndIndex: Int = (totalRecords * 0.8).toInt
    val validEndIndex: Int = (totalRecords * 0.9).toInt

    // 分割数据集为训练集、验证集和测试集
    val trainData: Dataset[Row] = sortedData.limit(trainEndIndex)
    val validData: Dataset[Row] = sortedData.limit(validEndIndex).except(trainData)
    val testData: Dataset[Row] = sortedData.except(trainData).except(validData)

    //设置模型参数 -- 协同过滤---最小二乘法
    val als: ALS = new ALS()
      //必选参数
      .setItemCol("MealEncoded")
      .setUserCol("UserEncoded")
      .setRatingCol("Rating")
      //可选参数
      .setRank(50)
      .setAlpha(0.01)
      .setMaxIter(10)
      .setImplicitPrefs(false)
      .setRegParam(0.3)

    val model: ALSModel = als.fit(trainData)
    //将模型里面的NaN数据删除

    model.setColdStartStrategy("drop")
    val pre: DataFrame = model.transform(testData)
    pre.show()

    val evaluator: RegressionEvaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("Rating")
      .setPredictionCol("prediction")
    val rmse: Double = evaluator.evaluate(pre)
    println(s"均方误差=$rmse")


    //为用户推荐前10名的菜品
    val userRecs: DataFrame = model.recommendForAllUsers(10)
    userRecs.show(truncate = false)
    //为菜品推荐前10的用户
    val mealRecs: DataFrame = model.recommendForAllItems(10)
    mealRecs.show(truncate = false)

    //未指定用户组生成10个网址推荐
    val users: Dataset[Row] = sortedData.select(als.getUserCol).distinct().limit(5)
    users.show(truncate = false)
    val userSubsetResc: DataFrame = model.recommendForUserSubset(users, 10)
    userSubsetResc.show(truncate = false)


    spark.stop()
  }
}
