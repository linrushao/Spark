package com.tipdm.test

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author linrushao
 * @Date 2023-08-10
 */
object Test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Test")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val law_data: DataFrame = spark.read.table("law.law_visit_log_all")
    law_data.show(10)


    spark.stop()
  }
}
