package com.tipdm.spark02_etl

import com.tipdm.common.CommonObject
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/***
  * 数据处理 02：?网址还原
  */

object SharePageData {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark = CommonObject.getHead("NextPage")
    val revertNextPageData = spark.read.table("law.revertNextPageData")
    revertNextPageData.show(5,false)
    //还原“？”网址
    val revertShareFunc = udf{(page:String,urlType:String)=>revertSharePageFunction(page,urlType)}

    val revertData = revertNextPageData.withColumn("url",revertShareFunc(col("url"),col("fullUrlId")))
    revertData.show(10,false)
    //revertData.filter("fullUrlID != 1999001").filter("fullUrl like '%?%'").select("fullUrl","url").show(20,false)
    revertData.distinct().write.mode("overwrite").saveAsTable("law.law_visit_log_all_cleaned")
    spark.stop()
  }

  def revertSharePageFunction(page:String,urlType:String): String ={
    if(urlType!="1999001"){
      if(page.contains("?")){
        page.substring(0,page.indexOf("?"))
      }else {
        page
      }
    }else {
      page
    }
  }



}
