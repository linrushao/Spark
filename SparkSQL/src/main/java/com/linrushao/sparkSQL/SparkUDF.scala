package com.linrushao.sparkSQL

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author linrushao
 * @Date 2023-08-07
 */
object SparkUDF {
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

    println("==========Hive table=========")
    val bigdata = spark.read.table("train.bigdata")
    val math: DataFrame = spark.read.table("train.math")
    bigdata.show()
    math.show()

    import spark.implicits._
    val movies_Data: DataFrame = sc.textFile("hdfs://master/data/movies.dat")
      .map(_.split("::"))
      .map(m => Movies(m(0).toInt, m(1), m(2)))
      .toDF()
    movies_Data.show()
    val rating_data: DataFrame = sc.textFile("hdfs://master/data/ratings.dat")
      .map(_.split("::"))
      .map(m => Ratings(m(0).toInt, m(1).toInt, m(2).toDouble, m(3).toLong))
      .toDF()
    rating_data.show()
    val user_data: DataFrame = sc.textFile("hdfs://master/data/users.dat")
      .map(_.split("::"))
      .map(m => Users(m(0).toInt, m(1).trim, m(2).toInt, m(3).trim, m(4).toInt))
      .toDF()
    user_data.show()

    /**
     * 自定义UDF函数，两种方式
     */
    // 方式一：用于SQL查询
    spark.udf.register("replace", (x: String) => {
      x match {
        case "M" => 0
        case "F" => 1
      }
    })
    user_data.select("userId","age","gender")
      .createOrReplaceTempView("tempView")
    spark.sql("select userId ,replace(gender) as sex from tempView")
      .show(5)
    //方式二：用DataFrame API
    import org.apache.spark.sql.functions._
    val genderReplace: UserDefinedFunction = udf((x: String) => {
      x match {
        case "M" => 0
        case "F" => 1
      }
    })
    user_data.select(col("userId"),genderReplace(col("gender")),col("age"))
      .show(5)

    /**
     * orderBy排序 三种方式
     */
    import org.apache.spark.sql.functions._
    //方式一：使用desc()方式



    spark.stop()
  }
}
