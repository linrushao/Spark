package com.linrushao.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spatk02_RDD_Collection_Par {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //TODO RDD -创建
    val list: List[Int] = List(1, 2, 3, 4)

    //并行
    //parallelize方法表示生成RDD数据处理模型
    //第一参数表示需要处理的数据源
    //第二个参数表示处理数据时默认的分区数量（并行度）
    //如果第二个参数没有传，那么使用默认值（1）
    //从配置对象中回去配置参数，如果获取不到，会设置为当前部署环境的总核数
    //local =>单核
    //local[3] = >三核
    //local => 表示使用当前环境最大核数
    val rdd: RDD[Int] = sc.parallelize(list, 2)

    //将数据保存到分区文件中
    rdd.saveAsTextFile("D:\\Atest\\inputWord")

    sc.stop()

  }

}
