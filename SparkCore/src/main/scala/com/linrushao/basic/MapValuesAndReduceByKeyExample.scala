package com.linrushao.basic

/**
 * @Author linrushao
 * @Date 2023-11-01
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object MapValuesAndReduceByKeyExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapValues and reduceByKey example").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Seq(("Hadoop",5), ("Java", 1), ("Python", 4), ("spark", 9), ("spark", 8), ("Python", 2), ("Java", 6), ("hadoop", 4), ("Python", 3)))
    val result = rdd.mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(x => x._1.toDouble / x._2.toDouble)
      .collect()
    result.foreach(println)
  }
}
