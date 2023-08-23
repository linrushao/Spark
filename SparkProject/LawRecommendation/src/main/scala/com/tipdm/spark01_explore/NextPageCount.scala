package com.tipdm.spark01_explore

import com.tipdm.common.CommonObject
import org.apache.spark.sql.functions._
/***
  * 数据探索4 ：翻页网站统计
  */
object NextPageCount {
  def main(args: Array[String]): Unit = {
    val spark = CommonObject.getHead("NextPage")
    //读取数据
    val lawData = spark.read.table("law.law_visit_log_all")
    val NextPageUdf = udf{(page:String,urlID:String)=>NextPageFunction.nextPageFunction(page,urlID)}

    val NextPageData = lawData.withColumn("NextPageTag",NextPageUdf(col("fullUrl"),col("fullUrlId")))
    NextPageData.select("fullUrl","fullUrlId","NextPageTag").show(false)
    
    println("翻页网页数量："+NextPageData.filter("NextPageTag==true").count())

    spark.stop()
  }
}
