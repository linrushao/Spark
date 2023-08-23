package com.linrushao

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @Author linrushao
 * @Date 2023-08-22
 */
object DataEncode {

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

    // 存储编码后的数据
    dataWithMealEncoding.select("UserEncoded", "UserID", "MealEncoded", "MealID", "Rating", "ReviewTime")
      .write
      .format("parquet")
      .mode("overwrite") // 覆盖已存在的数据
      .save("hdfs://master:8020/output/dataWithMealEncoding")

    // 存储用户元数据
    userEncodings.write
      .format("parquet")
      .mode("overwrite")
      .save("hdfs://master:8020/output/userEncodings")
    //存储菜品元数据
    mealEncodings.write
      .format("parquet")
      .mode("overwrite")
      .save("hdfs://master:8020/output/mealEncodings")


    spark.stop()
  }
}
