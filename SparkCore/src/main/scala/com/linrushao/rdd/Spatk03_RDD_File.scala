package com.linrushao.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spatk03_RDD_File {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //TODO RDD - 创建
    //从文件中创建RDD
    //File:文件、目录
    //textFile参数中path的含义，可以是具体的文件，也可以是目录
    val rdd: RDD[String] = sc.textFile("D:\\Atest\\inputWord\\abc.txt")
    rdd.collect.foreach(println)
    sc.stop()
  }
}
