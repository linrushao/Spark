package com.tipdm.spark01_explore

import org.apache.spark.sql.SparkSession
/***
  * 01:数据探索 统计记录数,用户数，网页数
  */
object CountExplore {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .appName("countExplore")
      .master("local[*]")
      .getOrCreate()
    //设置error等级日志输出
    spark.sparkContext.setLogLevel("Error")
    //获取数据表
    val law_data = spark.read.table("law.law_visit_log_all")
    //law_data.show(false)
    //数据探索
    println("数据量："+law_data.count())
    println("访问用户数："+law_data.select("userid").distinct().count())
    println("受访问网页数："+law_data.select("fullURL").distinct().count())

    spark.stop()

  }
}
