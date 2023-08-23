package com.tipdm.dataAnalysis

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}

/**
 * @Author linrushao
 * @Date 2023-08-11
 */
object DataProcessing {
  def main(args: Array[String]): Unit = {
    //构建sparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .appName("DataProcessing")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    //构建sparkContext
    val sc: SparkContext = spark.sparkContext

    //导入隐式转换
    import spark.implicits._

    //获取数据表
    val law_data: DataFrame = spark.read.table("law.law_visit_log_all")

    println("===========数据的总记录数=============")
    //数据的总记录数
    val law_data_count: Long = law_data.count()
    println("数据的总记录数：" + law_data_count)
    //用户数总的数量
    //select distinct userid from law.law_visit_log_all 进行用户去重
    val user_count: DataFrame = spark.sql(
      """select count('userid') as user_count
        |from (select distinct userid from law.law_visit_log_all)""".stripMargin
    )
    println("===========用户数总的数量=============")
    user_count.show()

    // 网页数
    val fullurl_count: DataFrame = spark.sql(
      """select count('fullurl') as fullurl_count
        |from (select distinct fullurl from law.law_visit_log_all)""".stripMargin
    )
    println("===========网页数=============")
    fullurl_count.show()

    //网页类别统计：统计每个网页类型所占的比例
    /**
     * 1.总的网页类型
     * select count('fullurlid') as fullurlid_count from law_visit_log_all
     * 2.单个网页类型
     * select fullurlid,count('fullurlid') as fullurlid_one from law_visit_log_all group by fullurlid)o1
     */
    val url_category_ratio: DataFrame = spark.sql(
      """select o1.fullurlid,fullurlid_one/t1.fullurlid_count as ratio
        |from (select fullurlid,count('fullurlid') as fullurlid_one
        |           from law.law_visit_log_all
        |           group by fullurlid)o1
        |join (select count('fullurlid') as fullurlid_count
        |           from law.law_visit_log_all)t1""".stripMargin
    )
    println("===========网页类别统计：统计每个网页类型所占的比例=============")
    url_category_ratio.show()

    /**
     * 重复数据探索
     * 1、总数据量重复数据
     * 2、每个字段的重复数据统计
     */
    //1.总数据量重复数据
    //总的网页数（去重后）
    // select count(*) distinct_count from (select distinct * from law.law_visit_log_all)t1;
    //总的网页数（没有去重）
    //select count(*)  as no_distinct_count from law.law_visit_log_all;
    val duplicate_data: DataFrame = spark.sql(
      """select n1.no_distinct_count-t2.distinct_count as duplicate_data
        |from (select count(*) distinct_count
        |         from (select distinct *
        |                 from law.law_visit_log_all)t1)t2
        |join (select count(*)  as no_distinct_count
        |         from law.law_visit_log_all)n1""".stripMargin
    )
    println("===========总数据量重复数据=============")
    duplicate_data.show()
    println("===========每个字段的重复数据统计=============")
    //2、每个字段的重复数据统计
    //获取所有字段名
    val columns: Array[String] = law_data.columns

    //对每个字段执行重复数据统计 <=1说明不重复
    for (column <- columns) {
      val duplicates_df: Dataset[Row] = law_data.groupBy(column).count().filter(col("count") > 1)

      duplicates_df.show()
    }

    //翻页网页分析
    // 1、统计翻页网站总数量 (例如： http://www.baidu.com/info/falv/123_4.html)
    // 定义一个UDF来判断是否满足条件
    spark.udf.register("isMatching", (url: String) => url.matches(".*_\\d+\\.html$"))

    // 统计包含下划线和数字的网址数量
    val filter_url: Dataset[Row] = law_data.filter(expr("isMatching(fullurl)"))
    val totalCount: Long = filter_url.count()
    println("===========统计包含下划线和数字的网址数量=============")
    print(totalCount)


    //网页网址的“？”分析
    //1、统计每个类型网址所占整个网址带?百分比
    //按类型分组，然后统计该类型中的总数，统计该类型中带？的总数，再利用？/总数

    // 按类型分组并统计
    val group_url_data: DataFrame = law_data.groupBy("fullurlid").agg(
      functions.count("*").as("total_urls"),
      sum(when(col("fullurl").contains("?"), 1).otherwise(0)).as("question_mark_count")
    )

    // 计算百分比
    val resultDF: DataFrame = group_url_data.withColumn("question_mark_percentage",
      (col("question_mark_count") / col("total_urls")) * 100
    )
    println("===========统计每个类型网址所占整个网址带?百分比=============")
    resultDF.show()

    /**
     * 异常数据处理规则
     * http://www.baidu.com/xx/xx/info.html?user=name&password=123456
     * http://www.baidu.com/xx/xx/info.html
     *  针对翻页网页，还原其为原始网页（“_数字.html”后缀的为翻页）。
     *  网址中包含“？”，同时其网页类别是非1999001的进行“？”的截取，保留“？”前的网址，进行网址还原。
     *  针对重复数据进行全字段重复记录去重。
     *  最后将数据预处理完的数据存储到Hive大数据仓库的law.law_visit_log_all_cleaned
     */

    // 针对翻页网页，还原其为原始网页（“_数字.html”后缀的为翻页）。
    // 定义一个UDF（User-Defined Function）来处理网页链接，将带有翻页后缀的链接还原为原始链接
    val restorePageUDF: UserDefinedFunction = udf((url: String) => {
      val originalUrl: String = if (url.matches(".*_\\d+\\.html$")) {
        url.replaceAll("_\\d+\\.html$", ".html")
      } else {
        url
      }
      originalUrl
    })

    // 利用UDF处理数据，添加一个新列来保存还原后的链接
    val processedData: DataFrame = law_data.withColumn("original_url", restorePageUDF($"fullurl"))
    println("===========针对翻页网页，还原其为原始网页=============")
    // 显示处理后的数据  truncate = false设置显示选项，将字符串列的截断长度设置为无限，以显示完整内容
    processedData.show(truncate = false)
    processedData.select("fullurl", "original_url").show(10, truncate = false)

    // 网址中包含“？”，同时其网页类别是非1999001的进行“？”的截取，保留“？”前的网址，进行网址还原。
    val intercept_url: UserDefinedFunction = udf((fullurlid: String, fullurl: String) => {
      if (fullurlid != "1999001" && fullurl.contains("?")) {
        val parts: Array[String] = fullurl.split("\\?")
        parts(0) // 返回 "?" 前的部分
      } else {
        fullurl
      }
    })
    println("=====网址中包含“？”，进行网址还原========")
    // 利用UDF处理数据，添加一个新列来保存处理后的网址
    val process_url: DataFrame = processedData.withColumn("intercept_url", intercept_url($"fullurlid", $"fullurl"))
    process_url.select("fullurlid", "fullurl", "intercept_url").filter("fullurlid != 1999001").show(numRows = 10, truncate = false)

    // 针对重复数据进行全字段重复记录去重
    val dropduplicate_data: Dataset[Row] = process_url.dropDuplicates()
    val no_duplicate_data: Long = dropduplicate_data.count()
    println("=====针对重复数据进行全字段重复记录去重========")
    println("所有数据：" + law_data_count)
    println("去重后数据：" + no_duplicate_data)

    // 最后将数据预处理完的数据存储到Hive大数据仓库的law.law_visit_log_all_cleaned
    println("=====保存数据到Hive表中========")
    dropduplicate_data.write.mode("overwrite").saveAsTable("law.law_visit_log_all_cleaned")

    println("数据保存成功")


    spark.stop()
  }
}
