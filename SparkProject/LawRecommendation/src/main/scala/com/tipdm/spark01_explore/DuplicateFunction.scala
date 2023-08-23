package com.tipdm.spark01_explore

import org.apache.spark.sql.DataFrame
object DuplicateFunction {

  /**
    * 统计每个字段数总量select(ColumnName).count()
    * 统计每个字不重复数据量select(ColumnName).distinct().count()
    * select(ColumnName).count() - select(ColumnName).distinct().count()
    * @param data
    * @param ColumnName
    */
  def duplicateFunction(data:DataFrame,ColumnName:String)={
    println(ColumnName+"重复数据量："+(data.select(ColumnName).count()
       -data.select(ColumnName).distinct().count()))
  }
}
