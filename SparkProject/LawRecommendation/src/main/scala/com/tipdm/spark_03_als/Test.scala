package com.tipdm.spark_03_als

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("db")
      .master("local[*]")
      .getOrCreate()

    val df = spark.createDataFrame(
      Seq((0,"a"),(1,"b"),(2,"c"),(3,"a"),(4,"a"),(5,"c"))
    ).toDF("id","category")

    val indexer =new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df) // 训练一个StringIndexer =&gt; StringIndexerModel
      .transform(df)  // 用 StringIndexerModel transfer 数据集

    indexed.show()

    spark.stop()
  }

}
