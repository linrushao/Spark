package com.tipdm.spark01_explore

import com.tipdm.common.CommonObject
/***
  * 02 网页类别统计
  */
object UrlType {
  def main(args: Array[String]): Unit = {
    val spark = CommonObject.getHead("UrlType")
    //读取表数据
    val law_data = spark.read.table("law.law_visit_log_all")
    import org.apache.spark.sql.functions._
    //统计每个网页id出现次数
    val UrlType = law_data.groupBy("fullUrlId").count()
    //UrlType.show(10,false)
      //求网页id出现次数占比，并存入weights字段中
     .withColumn("weights",round(col("count")/law_data.count()*100,3))
    //以weights倒序的形式输出
    UrlType.sort(desc("weights")).show()
    spark.stop()
  }
}
