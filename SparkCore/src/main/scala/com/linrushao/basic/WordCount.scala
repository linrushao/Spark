package com.linrushao.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author LinRuShao
 * @Date 2023/1/3 20:20
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建Spark配置，设置应用程序名称，如果是当地运行需要设置core核数
    val conf: SparkConf = new SparkConf().setAppName("WordCount") //.setMaster("local[2]")
    //创建Spark执行的入口
    val sc: SparkContext = new SparkContext(conf)
    //指定以后从哪里读取数据创建RDD【弹性分布式数据集】,第二个参数：设置分区数
    val lines: RDD[String] = sc.textFile("hdfs://master:8020/wordcount/words.txt", 1)
    //切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //将单词和一组合
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
    //按key进行聚合
    val reduce: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    //将结果保存到HDFS中
    reduce.saveAsTextFile("hdfs://master:8020/wordcountout")
    //释放资源
    sc.stop()
  }
}
