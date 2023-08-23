package com.linrushao.sparkSQL

/**
 * @Author linrushao
 * @Date 2023-08-07
 */

case class Movies(movieId:Int,title:String,Genres:String)
case class Users(userId:Int,gender:String,age:Int,occupation:String,zip:Int)
case class Ratings(userId:Int,movied:Int,rating:Double,timestamp:Long)
