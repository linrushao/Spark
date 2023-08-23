package com.linrushao.sparkSQL

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @Author linrushao
 * @Date 2023-08-08
 */
object HomeWork2 {
  case class Student(id: Int, name: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("HomeWork")
      .enableHiveSupport() //使用Hive数据库需要使用此配置
      .config("hive.metastore.uris", "thri1ft://master:9083") //读取不到Resource目录下的Hive配置，自己配置Hive的目标元数据库
      .getOrCreate()

    //创建SparkContext
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //导入隐式转换函数
    import spark.implicits._
    val studentRDD: DataFrame = sc
      .textFile("hdfs://master:8020/data/student.txt")
      .map(_.split("\t"))
      .map(x => Student(x(0).trim.toInt, x(1).trim))
      .toDF()
    //展示所有数据
    studentRDD.show()
    //展示学生id为1006的学生数据
    println("==========条件查询==========")
    studentRDD.where("id=1006").show()
    studentRDD.where("id>1009").show()
    studentRDD.where("id<=1003").show()
    //程序执行过程
    studentRDD.explain()
    //从后往前打印条数
    val rows: Array[Row] = studentRDD.tail(5)
    for (elem <- rows) {
      print(elem)
    }
    //去重
    studentRDD.distinct().show()

    spark.stop()
  }
}
