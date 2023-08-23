package com.linrushao.basic

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark03_WordCount1 {
  def main(args: Array[String]): Unit = {
    //TODO Spark WorkCount

    //TODO 1.连接Spark环境
    val sparkConf = new SparkConf()
    //环境
    sparkConf.setMaster("local")
    //应用程序的名称
    sparkConf.setAppName("WorkCount")
    val sc = new SparkContext(sparkConf)

    //TODO 2.操作数据，完成需求
    val lines: RDD[String] = sc.textFile("D:\\Atest\\inputWord\\abc.txt")
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //groupBy + mapValues => reduceByKey
    //A1 A1 => A1 ,_+_
    //根据key对Values数据聚合，根据key分组+对数据聚合
    //word => (word ,1)
    val wordToOne = words.map((_, 1))

    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    wordToCount.collect.foreach(println)

    //TODO 3.释放资源，关闭环境
    sc.stop()
  }


}
