package com.linrushao

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @Author linrushao
 * @Date 2023-08-18
 */
object airETL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("airETL")
      .enableHiveSupport() //使用Hive数据库需要使用此配置
      .config("hive.metastore.uris", "thrift://master:9083") //读取不到Resource目录下的Hive配置，自己配置Hive的目标元数据库
      .getOrCreate()

    import spark.implicits._
    //创建SparkContext
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //读取数据
    val airDF: DataFrame = spark.read.table("air.air_data_base")

    //(1)丢弃票价为空的记录,  将结果存储到sum_yr_1_not_null表
    val SUM_YR_1_is_not_null: Dataset[Row] = airDF.filter("SUM_YR_1 is not null")
    //    SUM_YR_1_is_not_null.write
    //      .mode("overwrite")
    //      .saveAsTable("air.sum_yr_1_not_null")

    //(2)丢弃票价为0、平均折扣率不为0、总飞行公里数大于0的记录，将结果存储到sum_0_seg_avg_not_0表
    val sum_0_seg_avg_not_0: Dataset[Row] = SUM_YR_1_is_not_null.filter("SUM_YR_1 == 0 and AVG_DISCOUNT != 0 and SEG_KM_SUM > 0")
    //    sum_0_seg_avg_not_0.write
    //      .mode("overwrite")
    //      .saveAsTable("air.sum_0_seg_avg_not_0")
    /**
     * （3） 对数据进行属性规约，从数据清洗结果中选择6个属性：FFP_DATE、LOAD_TIME、FLIGHT_COUNT、AVG_DISCOUNT、SEG_KM_SUM、LAST_TO_END，形成数据集，存储到flfasl表中
     */
    val flfasl: DataFrame = sum_0_seg_avg_not_0.select("FFP_DATE", "LOAD_TIME", "FLIGHT_COUNT", "AVG_DISCOUNT", "SEG_KM_SUM", "LAST_TO_END")
//        flfasl.write
//          .mode("overwrite")
//          .saveAsTable("air.flfasl")



    /**
     * （4） 对flfasl表的数据进行数据变换，构造LRFMC 5个指标，并将结果存储到lrfmc表中
     * L = LOAD_TIME - FFP_DATE
     * 会员入会时间距离观测窗口结束的月数=观测窗口的结束时间-入会时间 [单位：月]
     * R = LAST_TO_END
     * 客户最近一次乘坐公司飞机距观测窗口结束的月数 = 最后一次乘机时间至观察窗口末端时长[单位：月]
     * F = FLIGHT_COUNT
     * 客户在观测窗口内乘坐公司飞机的次数 = 观测窗口的飞行次数 [单位：次]
     * M = SEG_KM_SUM
     * 客户在观测时间内在公司累计的飞行里程 = 观测窗口总飞行公里数 [单位：公里]
     * C = AVG_DISCOUNT
     * 客户在观测时间内乘坐舱位所对应的折扣系数的平均值 = 平均折扣率 [单位：无]
     */

    // 将字符串日期列转换为日期类型
    // 构造L指标：LOAD_TIME - FFP_DATE
    val lrfmc = flfasl
      .withColumn("LOAD_TIME_DATE", to_date($"LOAD_TIME"))
      .withColumn("FFP_DATE_DATE", to_date($"FFP_DATE"))
      .withColumn("L", datediff($"LOAD_TIME_DATE", $"FFP_DATE_DATE"))
      .withColumn("L", round(expr("L/30"), 2))
      .withColumn("R", round(expr("LAST_TO_END/30"), 2))
      .withColumn("F", col("FLIGHT_COUNT"))
      .withColumn("M", col("SEG_KM_SUM"))
      .withColumn("C", col("AVG_DISCOUNT"))
    //    lrfmc.write
    //      .mode("overwrite")
    //      .saveAsTable("air.lrfmc")

    /**
     * （5） 根据lrfmc表，统计LRFMC 5个指标的取值范围[计算指标的最小值、最大值]
     */

    lrfmc.select(
      min($"L").alias("min_L"),
      max($"L").alias("max_L"),
      min($"R").alias("min_R"),
      max($"R").alias("max_R"),
      min($"F").alias("min_F"),
      max($"F").alias("max_F"),
      min($"M").alias("min_M"),
      max($"M").alias("max_M"),
      min($"C").alias("min_C"),
      max($"C").alias("max_C"),
    ).show()


    spark.stop()
  }
}
