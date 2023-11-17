package com.linrushao.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object Spatk04_RDD_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //TODO RDD -构建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //TODO 转换算子-map
    //Scala：list.map(fn) => 将旧的集合转换为新的集合，其中的元素要执行转换逻辑集合中每一个元素执行的转换逻辑
    //spark：RDD.map(fn) =>将旧的RDD转换为新的RDD，其中的元素要执行转换逻辑数据源中每一个元素执行的转换逻辑
    //这里所谓的转换，其实就i是构建一个新的RDD，但是会将旧的RDD给包起来，这样就形成了一种特定的设计模式，装饰者设计模式，体现了功能的扩展和叠加
    //val newRDD :RDD[Int] = rdd.map(num=>num*2)
    val newRDD: RDD[Int] = rdd.map(_ * 2)
    newRDD.collect.foreach(println)
    sc.stop()
  }
}
