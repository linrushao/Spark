package com.tipdm.common

import org.apache.spark.sql.SparkSession

object CommonObject {


  def getHead(appName:String):SparkSession={
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
    //设置错误日志级别
    spark.sparkContext.setLogLevel("Error")
    return spark
  }

}
