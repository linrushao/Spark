package com.linrushao.basic

object Spark02_Test {
  def main(args: Array[String]): Unit = {
    val list = List(
     List( List(1, 2), List(3,4)),
      List(List(5,6),List(7,8))
    )
    //扁平化并不会将所有的数据集合全部拆开
    //只能对当前集合的元素进行扁平操作
    list.flatMap(list=>list).foreach(println)
  }

}
