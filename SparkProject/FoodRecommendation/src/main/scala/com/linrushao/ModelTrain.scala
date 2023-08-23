package com.linrushao

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.functions.{dense_rank, max}

/**
 * @Author linrushao
 * @Date 2023-08-22
 */
object ModelTrain {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("DataLoad")
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
    println("训练集：" + trainData.count())
    println("验证集：" + validData.count())
    println("测试集：" + testData.count())

    //保存数据集
    trainData.write
      .format("parquet")
      .mode("overwrite")
      .save("hdfs://master:8020/output/trainData")

    validData.write
      .format("parquet")
      .mode("overwrite")
      .save("hdfs://master:8020/output/validData")

    testData.write
      .format("parquet")
      .mode("overwrite")
      .save("hdfs://master:8020/output/testData")


    spark.stop()
  }
}
