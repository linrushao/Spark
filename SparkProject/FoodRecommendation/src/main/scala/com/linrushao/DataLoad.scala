package com.linrushao

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._


/**
 * @Author linrushao
 * @Date 2023-08-22
 */
object DataLoad {
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
    //    //显示前5条记录
    //    println("========显示前5条记录========")
    //    mealDF.show(5)
    //
    //    println("========数据统计========")
    //    // 总记录数
    //    val count: Long = mealDF.count()
    //    println("总记录数：" + count)
    //
    //    // 总用户数
    //    val usersCount: Long = mealDF.select("UserID").distinct().count()
    //    println("总用户数：" + usersCount)
    //
    //    // 总菜品数
    //    val mealCount: Long = mealDF.select("MealID").distinct().count()
    //    println("总菜品数：" + mealCount)
    //
    //    // 最低评分与最高评分
    //    mealDF.createOrReplaceTempView("meal_list")
    //    val scoreMax: DataFrame = spark.sql("select Rating from meal_list order by Rating desc limit 1")
    //    val scoreMin: DataFrame = spark.sql("select Rating from meal_list order by Rating asc limit 1")
    //    println("最低评分")
    //    scoreMin.show()
    //    println("最高评分")
    //    scoreMax.show()
    //
    //    println("========各级评分的分组统计========")
    //    val ratingGroup: DataFrame = mealDF.groupBy("Rating").count()
    //    //自定义udf函数
    //    val ratingGrage: UserDefinedFunction = udf((ratingCount: Int) => {
    //      ratingCount / count.toDouble
    //    })
    //
    //    val rating: DataFrame = ratingGroup.withColumn("Proportion", ratingGrage(col("count")))
    //    rating.show()

    //
    //    println("========评分次数统计========")
    //    // 各用户的评分次数统计，返回评分次数最多的前5名用户
    //    val userRating: DataFrame = mealDF.groupBy("UserID")
    //      .count()
    //      .sort(desc("count"))
    //      .limit(5)
    //    userRating.show()
    //
    //    // 各菜品的评分次数统计，返回评分次数最多的前5名菜品
    //    val mealRating: DataFrame = mealDF.groupBy("MealID")
    //      .count()
    //      .sort(desc("count"))
    //      .limit(5)
    //    mealRating.show()

    //    println("========按日期分组统计数据分布========")
    //    // 统计最早评分日期与最晚评分日期
    //    // 按日期统计用户评分的分布
    //    val toDate: UserDefinedFunction = udf((timeStamp: Int) => {
    //      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timeStamp * 1000)
    //    })
    //    println("最早日期")
    //    val earliestTime: Dataset[Row] = mealDF.select("ReviewTime")
    //      .withColumn("Date",toDate(col("ReviewTime")))
    //      .sort(asc("Date"))
    //      .limit(1)
    //    earliestTime.show()
    //    println("最晚日期")
    //    val latestTime: Dataset[Row] = mealDF.select("ReviewTime")
    //      .withColumn("Date",toDate(col("ReviewTime")))
    //      .sort(desc("Date"))
    //      .limit(1)
    //    latestTime.show()
    //
    //    val toDate1: UserDefinedFunction = udf((timeStamp: Int) => {
    //      new SimpleDateFormat("yyyy-MM-dd").format(timeStamp * 1000)
    //    })
    //    val mealAndDateDF: DataFrame = mealDF.withColumn("Date", toDate1(col("ReviewTime")))
    //    val DataGroup: DataFrame = mealAndDateDF.groupBy("Date").count()
    //    DataGroup.show()

        println("========统计用户重复评分的记录========")
        // 使用窗口函数标记重复记录
        val windowSpec: WindowSpec = Window.partitionBy("MealID", "UserID").orderBy(col("ReviewTime"))
        val dataWithDuplicates: DataFrame = mealDF.withColumn("rank", rank().over(windowSpec))

        // 统计重复记录数量
        val duplicateCount: Long = dataWithDuplicates.filter(col("rank") > 1).count()
        println(s"重复记录总数: $duplicateCount")

        println("========创建重复记录集的DataFrame========")
        // 创建临时表保存重复记录
        dataWithDuplicates.createOrReplaceTempView("duplicates")
        // 查询重复记录的特性
        val duplicateRecords: DataFrame = spark.sql(
          """
            |SELECT *
            |FROM duplicates
            |WHERE rank > 1
              """.stripMargin)
        // 显示查询结果
        duplicateRecords.show()
        val duplicateRecordsTest: Dataset[Row] = mealDF.filter(col("UserID").equalTo("A72BEKNJCGJA8"))
        duplicateRecordsTest.show()


    println("========删除重复的评分数据========")
    // 使用时间戳最大值来获取最新评分记录
    val latestRatings: DataFrame = mealDF.groupBy("MealID", "UserID")
      .agg(max("ReviewTime").alias("ReviewTime"))
      .join(mealDF, Seq("MealID", "UserID", "ReviewTime"))

//    //验证是否删除
//    // 使用窗口函数标记重复记录
//    val windowSpec: WindowSpec = Window.partitionBy("MealID", "UserID").orderBy(col("ReviewTime"))
//    val dataWithDuplicates: DataFrame = latestRatings.withColumn("rank", rank().over(windowSpec))
//
//    // 统计重复记录数量
//    val duplicateCount: Long = dataWithDuplicates.filter(col("rank") > 1).count()
//    println(s"重复记录总数: $duplicateCount")


    val value: Dataset[Row] = latestRatings.filter(col("UserID").equalTo("A72BEKNJCGJA8"))
    value.show()

    // 显示提取后的最新评分记录
    //latestRatings.show()

    println("========保存去重后的用户评分记录到hdfs中========")
    // 保存去重后的评分记录到HDFS
    latestRatings.write
      .format("csv")
      .mode("overwrite") // 覆盖已存在的数据
      .save("hdfs://master:8020/output/duplicateMeal")

    spark.stop()
  }
}
