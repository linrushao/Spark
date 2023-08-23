package com.tipdm.spark_03_als

import com.tipdm.common.CommonObject
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.recommendation.ALSModel
/**
  * 04 为用户推荐
  */
object Recommend {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark = CommonObject.getHead("Recommend")

    //模型的调用
    val model: PipelineModel = PipelineModel.load("hdfs://master:8020/model/ALS")
    //为用户推荐前10名的电影
    print(model)
    //val userRecs = model.recommendForAllUsers(10)

    spark.stop()
  }

}
