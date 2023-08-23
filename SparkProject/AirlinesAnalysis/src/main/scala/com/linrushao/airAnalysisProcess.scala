package com.linrushao

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, desc, sum}

/**
 * @Author linrushao
 * @Date 2023-08-18
 */
object airAnalysisProcess {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("airAnalysis")
      .enableHiveSupport() //使用Hive数据库需要使用此配置
      .config("hive.metastore.uris", "thrift://master:9083") //读取不到Resource目录下的Hive配置，自己配置Hive的目标元数据库
      .getOrCreate()

    //创建SparkContext
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //读取数据
    val airDF: DataFrame = spark.read.table("air.air_data_base")

    //（3） 数据导入完成后，统计SUM_YR_1、SEG_KM_SUM、AVG_DISCOUNT三个字段的空值记录数。
    val sum_all: Long = airDF.count()
    val sum_yr_1_null: Long = sum_all - airDF.filter("SUM_YR_1 is not null").count()
    val SEG_KM_SUM_null: Long = sum_all - airDF.filter("SEG_KM_SUM is not null").count()
    val AVG_DISCOUNT_null: Long = sum_all - airDF.filter("AVG_DISCOUNT is not null").count()
    println("sum_yr_1的空值记录数：" + sum_yr_1_null)
    println("SEG_KM_SUM的空值记录数：" + SEG_KM_SUM_null)
    println("AVG_DISCOUNT的空值记录数：" + AVG_DISCOUNT_null)

    //（4） 统计票价SUM_YR_1为0，平均折扣率AVG_DISCOUNT不为0，总飞公里数SEG_KM_SUM大于0的消费记录异常数据。
    val outlierData: Long = airDF.filter("SUM_YR_1 == 0 and AVG_DISCOUNT != 0 and SEG_KM_SUM > 0").count()
    println("异常数据：" + outlierData)


    spark.stop()
  }
}
