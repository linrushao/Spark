package com.tipdm.spark01_explore

import com.tipdm.common.CommonObject
import org.apache.spark.sql.DataFrame

/***
  * 03 数据统计：重复数据统计
  */
object DuplicateDataCount {
  def main(args: Array[String]): Unit = {

    val spark = CommonObject.getHead("DuplicateDataCount")
    //读取数据
    val lawData = spark.read.table("law.law_visit_log_all")
    //获取law_visit_log_all表的所有字段名
    val columnsName = lawData.columns.toList
    println(columnsName)
    //字段的重复数据统计
    for(i<-columnsName){
      //DuplicateFunction.duplicateFunction(lawData,i)
      println(i+"重复数据量："+(lawData.select(i).count()
        -lawData.select(i).distinct().count()))
    }

    //总数据的重复数据统计
    println("总数据的重复数据："+(lawData.count()-lawData.distinct().count()))

    spark.stop()
  }
}
