package com.tipdm.spark_03_als

import com.tipdm.common.CommonObject
import com.tipdm.spark_03_als.ALSModel.{StringChange, ratingFunction}
import org.apache.spark.sql.functions.{col, count, desc, udf}
import org.apache.spark.sql.types.DoubleType

/***
  * 01 数据模型准备
  */
object DataModel {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark = CommonObject.getHead("ALSModel")

    val lawData = spark.read.table("law.law_visit_log_all_cleaned")
    //lawData.show(10,false)

    //对userid和网址进行数值编码（String->float）
    val FilterData = lawData.filter("fullUrlId==101003")
    //FilterData.show(10,false)
    val UserDataEncoding = StringChange(FilterData,"userid","userIdEncoding")
    //UserDataEncoding.select("userid","url","userIdEncoding").orderBy("userIdEncoding").show(100,false)
    val EncodingData = StringChange(UserDataEncoding,"fullurl","UrlEncoding")
    //EncodingData.show(5,false)

  /*  //每个用户点击次数
    EncodingData.groupBy("userIdEncoding").agg(count("userIdEncoding") as "userClicks")
      //统计每个点击次数的次数
      .groupBy("userClicks").agg(count("userIdEncoding") as "userClicksCount"
      //用户的占比百分比
      ,count("userIdEncoding")/EncodingData.select("userIdEncoding").distinct().count()*100 as "userPercent"
      //网址的占比百分比
      ,count("userIdEncoding")/EncodingData.count()*100 as "urlPercent")
      //userPercent倒序排序
      .orderBy(desc("userPercent")).show()

    //rating字段的构建
    //构建udf评分映射函数
    val ratingFunc = udf{(count:Int)=>ratingFunction(count)}
    //统计用户访问次数
    val countEncoding = EncodingData.groupBy("userIdEncoding").count()//userid,count
    countEncoding.show(10,false)
    //构建一个含userIdEncoding，UrlEncoding，count字段
    val thanOne = countEncoding.join(EncodingData,"userIdEncoding").filter("count>1").distinct()
    thanOne.show(10,false)
    //根据Count构建了评分映射
    val ModeData = thanOne.withColumn("label",ratingFunc(col("count")).cast(DoubleType)).select("userIdEncoding","UrlEncoding","label")
    ModeData.show(10,false)*/

    //ModeData.write.mode("overwrite").saveAsTable("law.law_model")

    spark.stop()
  }

}
