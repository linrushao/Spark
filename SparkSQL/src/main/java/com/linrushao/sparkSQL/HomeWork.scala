package com.linrushao.sparkSQL

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author linrushao
 * @Date 2023-08-08
 */
object HomeWork {
  case class Student(id: Int, name: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("HomeWork")
      .enableHiveSupport() //使用Hive数据库需要使用此配置
      .config("hive.metastore.uris", "thrift://master:9083") //读取不到Resource目录下的Hive配置，自己配置Hive的目标元数据库
      .getOrCreate()

    //创建SparkContext
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //在利用反射机制推断RDD模式时
    //导入隐式转换函数
    import spark.implicits._
    val StudentRDD: DataFrame = sc
      .textFile("hdfs://master:8020/data/student.txt")
      .map(_.split("\t"))
      .map(x => Student(x(0).trim.toInt, x(1).trim))
      .toDF()
    StudentRDD.show()

    //使用case class前提是知道字段的名称，但在有些情况下，是不知道字段的名称。这时候无法提前定义case class，这时候就需要采用编程方式定义RDD模式。
    import org.apache.spark.sql._
    val StudentRDD2: RDD[String] = sc.textFile("hdfs://master:8020/data/student.txt")
    //定义模式字符串
    val schemaString = "studentID name"
    //根据模式字符串生成模式
    val fields: Array[StructField] = schemaString
      .split("\t")
      .map(x => StructField(x, StringType, nullable = true))
    //模式对象StructureField传递给StructureTypes对象，模式已经创建成功
    val structType: StructType = StructType(fields)
    //将RDD的每一行内容创建为Row对象
    val rowRDD: RDD[Row] = StudentRDD2
      .map(_.split("\t"))
      .map(x => Row(x(0).trim, x(1).trim))
    print("自定义RDD模式")
    rowRDD.foreach(println(_))
    spark.stop()
  }
}
