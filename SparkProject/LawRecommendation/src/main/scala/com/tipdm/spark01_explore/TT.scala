package com.tipdm.spark01_explore

import org.apache.spark.ml.feature.StringIndexer
import com.tipdm.common.CommonObject

object TT {
  def main(args: Array[String]): Unit = {
    val spark = CommonObject.getHead("NextPage")
    import spark.implicits._
    val df = spark.createDataFrame(Seq(
      (0, "log"),
      (1, "text"),
      (2, "text"),
      (3, "soyo"),
      (4, "text"),
      (5, "log"),
      (6, "log"),
      (7, "log")
    )).toDF("id", "type")
    val indexer = new StringIndexer().setInputCol("type").setOutputCol("type_index")
    val model = indexer.fit(df)
    model.labels.foreach(println)   //类型的频率顺序(高-->低)
    val index = model.transform(df) //索引先排频率高的即log为0
    index.show(false)

    spark.stop()
  }
}
