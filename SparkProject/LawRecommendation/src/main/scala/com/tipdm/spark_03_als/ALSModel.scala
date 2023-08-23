package com.tipdm.spark_03_als

import com.tipdm.common.CommonObject
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** *
 * 构建数据模型
 */

object ALSModel {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark = CommonObject.getHead("ALSModel")

    val lawData = spark.read.table("law.law_visit_log_all_cleaned")
    //lawData.show(10,false)

    //对userid和网址进行数值编码（String->float）
    val FilterData = lawData.filter("fullUrlId==101003")
    //FilterData.show(10,false)
    val UserDataEncoding = StringChange(FilterData, "userid", "userIdEncoding")
    //UserDataEncoding.select("userid","url","userIdEncoding").orderBy("userIdEncoding").show(100,false)
    val EncodingData = StringChange(UserDataEncoding, "fullurl", "UrlEncoding")
    //EncodingData.show(5,false)

    //每个用户点击次数
    EncodingData.groupBy("userIdEncoding").agg(count("userIdEncoding") as "userClicks")
      //统计每个点击次数的次数
      .groupBy("userClicks").agg(count("userIdEncoding") as "userClicksCount"
      //用户的占比百分比
      , count("userIdEncoding") / EncodingData.select("userIdEncoding").distinct().count() * 100 as "userPercent"
      //网址的占比百分比
      , count("userIdEncoding") / EncodingData.count() * 100 as "urlPercent")
      //userPercent倒序排序
      .orderBy(desc("userPercent")).show()

    //rating字段的构建
    //构建udf评分映射函数
    val ratingFunc = udf { (count: Int) => ratingFunction(count) }
    //统计用户访问次数
    val countEncoding = EncodingData.groupBy("userIdEncoding").count() //userid,count
    //countEncoding.show(10,false)
    //构建一个含userIdEncoding，UrlEncoding，count字段
    val thanOne = countEncoding.join(EncodingData, "userIdEncoding").filter("count>1").distinct()
    //thanOne.show(10,false)
    //根据Count构建了评分映射
    val ModeData = thanOne.withColumn("label", ratingFunc(col("count")).cast(DoubleType)).select("userIdEncoding", "UrlEncoding", "label")
    //ModeData.show(10,false)

    //用户访问单个网页的次数
    ModeData.groupBy("userIdEncoding", "UrlEncoding")
      //用户访问某一网页的具体次数
      .agg(count("userIdEncoding") as "clicks")
      //根据求出的具体次数进行再次统计
      .groupBy("clicks").count()
      //计算百分比占比
      .withColumn("percent", col("count") / ModeData.distinct().count() * 100)
      //倒序的排序
      .sort(desc("count")).show()

    //数据切分==>切分训练集 和 测试集
    //训练集 做相关的训练
    val Array(train, test) = ModeData.randomSplit(Array(0.8, 0.2))
    println("train数据量：" + train.count(), "test数据量：" + test.count())
    //数据保存
    //train.write.mode("overwrite").saveAsTable("law.train")

    //设置模型参数 -- 协同过滤---最小二乘法
    val als = new ALS()
      //必选参数
      .setItemCol("UrlEncoding")
      .setUserCol("userIdEncoding")
      .setRatingCol("label")
      //可选参数
      .setRank(10)
      .setAlpha(1.0)
      .setMaxIter(8)
      .setImplicitPrefs(false)
      .setRegParam(0.09)

    //参数封装
    val paraGrid = new ParamGridBuilder()
      .addGrid(als.alpha, Array(0.3, 1.0, 3.0))
      .addGrid(als.maxIter, Array(8, 10, 12))
      .addGrid(als.rank, Array(8, 10, 12))
      .addGrid(als.implicitPrefs, Array(true, false))
      .addGrid(als.regParam, Array(0.1, 0.09, 0.3))
      .build()
    //交叉验证
    val cv = new CrossValidator()
      .setEstimator(als)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paraGrid)
      .setNumFolds(2)
    val model1 = cv.fit(train)
    val pre = model1.transform(test)
    print(pre)
    val model = als.fit(train)
    //将模型里面的NaN数据删除

    model.setColdStartStrategy("drop")
    //val pre = model.transform(test)
    //模型的保存
    //model.save("hdfs://192.168.10.100:8020/model/ALS")
    //模型的调用
    //PipelineModel.load("")
    pre.show()
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("label")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(pre)
    println(s"均方误差=$rmse")


    //----------
    //为用户推荐前10名的电影
    val userRecs: DataFrame = model.recommendForAllUsers(10)

    //为网址推荐前10的用户
    val UrlReces = model.recommendForAllItems(10)

    //未指定用户组生成10个网址推荐
    val users = ModeData.select(als.getUserCol).distinct().limit(3)
    val userSubsetResc = model.recommendForUserSubset(users, 10)

    //指定网址生成10个用户推荐
    //val urls = ModeData.select(als.getItemCol).distinct().limit(3)
    //val urlSubsetResc = model.recommendForItemSubset(urls,10)


    //推荐结果保存
    // userRecs.write.mode("overwrite").saveAsTable("law.userResc")
    // userSubsetResc.write.mode("overwrite").saveAsTable("law.userSubsetResc")
    //保存到hdfs
    /*UrlReces.write.mode("overwrite").save("hdfs://192.168.128.130:8020/Als/url/")
    urlSubsetResc.write.mode("overwrite").save("hdfs://192.168.128.130:8020/Als/url/")*/
  }

  //索引先排频率高的即log为0
  def StringChange(data: DataFrame, inputColumnName: String, outPutColumnName: String): DataFrame = {
    val Change = new StringIndexer()
      .setInputCol(inputColumnName)
      .setOutputCol(outPutColumnName)
    val changeData = Change.fit(data).transform(data)
    changeData
  }

  def ratingFunction(count: Int): Double = {
    if (count <= 8) {
      count
    } else if (count > 12) {
      10
    } else {
      9
    }
  }

}
