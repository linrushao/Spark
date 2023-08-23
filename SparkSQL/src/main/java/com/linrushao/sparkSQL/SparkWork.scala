package com.linrushao.sparkSQL

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{ avg, desc, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author linrushao
 * @Date 2023-08-07
 */
object SparkWork {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("MySql Conn Test")
      .enableHiveSupport() //使用Hive数据库需要使用此配置
      .config("hive.metastore.uris", "thrift://master:9083") //读取不到Resource目录下的Hive配置，自己配置Hive的目标元数据库
      .getOrCreate()

    //创建SparkContext
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")


    val bigdata: DataFrame = spark.read.table("train.bigdata")
    val math: DataFrame = spark.read.table("train.math")
    bigdata.show()
    math.show()

    //关联bigdata和math
    //根据学生ID分组统计成绩平均分和总分
    //并根据总分进行降序排序
    bigdata.unionAll(math)
      .groupBy("id")
      .agg(sum("score") as "allscore", avg("score") as "avgscore")
      .orderBy(desc("allscore"))


    //保存数据

    spark.stop()
  }
}
