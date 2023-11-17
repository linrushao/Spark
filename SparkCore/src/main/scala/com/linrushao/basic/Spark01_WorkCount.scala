package com.linrushao.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WorkCount {
  def main(args: Array[String]): Unit = {
    //TODO Spark WorkCount

    //TODO 1.连接Spark环境
    val sparkConf = new SparkConf()
    //环境
    sparkConf.setMaster("120.79.35.91")
    //应用程序的名称
    sparkConf.setAppName("WorkCount")
    val sc = new SparkContext(sparkConf)

    //TODO 2.操作数据，完成需求
    //2.1 从文件中读取原始数据
    //file => lines
    //spark读取文件默认以行为单位读取
    val lines: RDD[String] = sc.textFile("D:\\Atest\\inputWord\\abc.txt")

    //2.2 将原始数据分解成一个一个的单词（分词）
    //扁平化
    //line => word
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //2.3 将分成后的数据根据单词进行分组
    //(word,List(word))
    val group: RDD[(String, Iterable[String])] = words.groupBy(words => words)

    //2.4 将分组后的数据进行聚合
    //(word.List(word)) => (word,size)
    val wordCount: RDD[(String, Int)] = group.mapValues(list => list.size)

    //2.5 将聚合的结果采集到控制台打印出来
    //Scala：collect方法支持偏函数
    wordCount.collect.foreach(println)

    //TODO 3.释放资源，关闭环境
    sc.stop()
  }
}

