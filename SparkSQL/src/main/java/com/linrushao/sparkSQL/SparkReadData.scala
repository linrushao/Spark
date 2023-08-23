package com.linrushao.sparkSQL

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author linrushao
 * @Date 2023-08-07
 */
object SparkReadData {
  case class Person(name: String, age: Int)

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

    //parquet数据文件作为数据来源创建为DataFrame
    println("========parquet=========")
    val input = "hdfs://master:8020/data/users.parquet"
    val df_parquet: DataFrame = spark.read.parquet(input)
    df_parquet.show()


    //JSON文件作为数据来源创建为DataFrame
    println("========JSON=========")
    val df_json: DataFrame = spark.read.json("hdfs://master:8020/data/people.json")
    df_json.show()

    //txt数据文件作为数据来源创建为DataFrame
    println("========txt=========")
    val df_txt: DataFrame = spark.read.text("hdfs://master:8020/data/people.txt")
    df_txt.show()

    //csv数据文件作为数据来源创建为DataFrame
    println("========csv=========")
    val df_csv: DataFrame = spark.read.csv("hdfs://master:8020/data/people.csv")
    df_csv.show()

    println("========csv2=========")
    val df_csv2: DataFrame = spark
      .read
      .option("header", "true")
      .option("sep", ";")
      .csv("hdfs://master:8020/data/people.csv")
    df_csv2.show()

    // 测试MySQL作为数据源
    print("==========MySQL============")
    import java.util.Properties
    val pro = new Properties()
    pro.put("driver", "com.mysql.cj.jdbc.Driver")
    pro.put("user", "root")
    pro.put("password", "linrushao")
    val df_jdbc = spark.read.jdbc("jdbc:mysql://master:3306", "hive.TYPES", pro)
    df_jdbc.show()


    //从RDD创建dataFrame
    //方式一：利用反射机制推断RDD模式：通过反射，让Spark自动获取样例类中的名称、类型等信息，进行转换
    //导入隐式转换函数
    println("==========反射========")
    import spark.implicits._
    val peopleRDD: DataFrame = sc.textFile("hdfs://master:8020/data/people.txt")
      .map(_.split(","))
      .map(x => Person(x(0).trim, x(1).trim.toInt))
      .toDF()
    peopleRDD.show()

    //方式二：使用编程方式定义RDD模式
    //1.从原始RDD创建一个元素为Row类型的RDD
    //2.用StructType创建一个和RDD中的Row的结构相匹配的schema
    //3.通过sparkseesion提供的createDataFrame方法将schema应用到RDD上
    println("==========自定义模式========")
    import org.apache.spark.sql._
    val peopleRDD2: RDD[String] = sc.textFile("hdfs://master:8020/data/people.txt")
    //定义模式字符串
    val schemaString = "name age"
    //根据模式字符串生成模式
    val fields: Array[StructField] = schemaString
      .split(" ")
      .map(x => StructField(x, StringType, nullable = true))
    //模式对象StructureField传递给StructureTypes对象，模式已经创建成功
    val structType: StructType = StructType(fields)
    //将RDD的每一行内容创建为Row对象
    val rowRDD: RDD[Row] = peopleRDD2
      .map(_.split(","))
      .map(x => Row(x(0).trim, x(1).trim))
    //将RDD转换为DataFrame，将自定义模式和Row的RDD作为参数传入
    val peopleDF: DataFrame = spark.createDataFrame(rowRDD, structType)
    peopleDF.show()

    //4.从Hive表创建DataFrame，两种方式
    //方式1：通过SQL查询语句，Spark.sql()
    println("==========Spark sql=========")
    val hiveSQL: DataFrame = spark.sql("select * from shop.goodsorder")
    hiveSQL.show()
    //方式2：通过table方法 Spark.read.table(tablename)
    println("==========Hive table=========")
    val hiveTable = spark.read.table("shop.goodsorder")
    hiveTable.show(10)

    spark.stop()
  }
}
