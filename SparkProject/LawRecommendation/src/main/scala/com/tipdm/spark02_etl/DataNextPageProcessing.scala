package com.tipdm.spark02_etl

import com.tipdm.common.CommonObject
import com.tipdm.spark01_explore.NextPageFunction
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/***
  * 数据处理 01：翻页网址还原
  */

object DataNextPageProcessing {

  def main(args: Array[String]): Unit = {
    val spark = CommonObject.getHead("DataNextPageProcessing")

    val lawData = spark.read.table("law.law_visit_log_all")
    //构建翻页网址的判别标签
    val NextPageFunc = udf{(page:String,urlId:String)=>NextPageFunction.nextPageFunction(page,urlId)}

    val NextPage = lawData.withColumn("NextPageTag",NextPageFunc(col("fullUrl"),col("fullUrlId")))
    NextPage.show(10,false)

    //还原翻页网址
    val revertPage = udf{(page:String,NextPageTag:Boolean)=>revertPageFunction(page,NextPageTag)}

    val revertPageData = NextPage.withColumn("url",revertPage(col("fullUrl"),col("NextPageTag")))
    revertPageData.show(10,false)
    revertPageData.write.mode("overwrite").saveAsTable("law.revertNextPageData")
  }
  //http://www.***.cn/info/hunyin/hunyinfagui/201404102884290_6.html  转换为下面
  //http://www.***.cn/info/hunyin/hunyinfagui/201404102884290.html
  def revertPageFunction(page:String,NextPageTag:Boolean)={
    if(NextPageTag){
      val splitIndex = page.lastIndexOf("_")
      page.substring(0,splitIndex)+".html"
    }else {
      page
    }
  }

}
