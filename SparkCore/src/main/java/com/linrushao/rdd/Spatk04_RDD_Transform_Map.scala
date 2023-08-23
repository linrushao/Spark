package com.linrushao.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spatk04_RDD_Transform_Map {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //TODO RDD -构建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    //TODO 转换算子-map
    //1.转换后分区不变
    //2.数据处理后，分区不变
    //3.分区间数据无序
    //4.分区内数据有序
    //5.分区内数据迭代式操作（串行）
    val newRDD: RDD[Int] = rdd.map(_ * 2)
    newRDD.collect.foreach(println)

    sc.stop()
  }
}
