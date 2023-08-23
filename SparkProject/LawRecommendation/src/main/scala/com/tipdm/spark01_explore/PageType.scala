package com.tipdm.spark01_explore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/***
  *05 数据探索，统计网址带？的统计
  */
object PageType {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .appName("PageType")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    val lawData = spark.read.table("law.law_visit_log_all")
    val fileData = lawData.filter("fullUrl like '%?%'");

    val pageType = fileData.groupBy("fullUrlId").count()
      .withColumn("weights",round(col("count")/fileData.count()*100,3))
    pageType.sort(desc("count")).show()

    lawData.select("fullUrl","fullUrlId")
      .filter("fullUrl like '%?%'")
      .show(5,false)
  }

}
