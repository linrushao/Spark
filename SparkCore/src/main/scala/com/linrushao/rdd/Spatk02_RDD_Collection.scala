package com.linrushao.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object Spatk02_RDD_Collection {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //TODO RDD -创建
    //1)从集合（内存）中创建
    val list = List(1, 2, 3, 4)
    //RDD中的泛型表示分区处理时数据类型
    //parallelize => list.par.map
    //并行
    val rdd: RDD[Int] = sc.parallelize(list)

    //make:生成RDD，制作RDD
    //makeRDD底层其实就是调用parallelize方法，所以两个方法时一样的
    val rdd1: RDD[Int] = sc.makeRDD(list)

    rdd.collect.foreach(println)

    sc.stop()

  }

}
