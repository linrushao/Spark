package com.tipdm.spark01_explore

import org.apache.commons.lang.StringUtils
/**
  * 数据探索4 ：判断网址是否是翻页
  */
object NextPageFunction {
  // NextPageFunction("www.***.cn/info/laodong/zhiyebing/20140304141989_2.html","107002")
  def nextPageFunction(page:String,urlType:String):Boolean={
    if (!(urlType=="107001" || page == null || page.length<6)){
      return false
      //http://www.***.cn/info/laodong/zhiyebing/20140304141989_2.html
    }else if (page.contains(".html") && page.contains("_")){
      val before = page.lastIndexOf("_")
      val end = page.lastIndexOf(".")
      if (before>=0 && end>before+1){
        val num = page.substring(before+1,end)
        return samllInit(num)
      }
    }
    return false
  }


  def samllInit(num:String):Boolean={
    if(StringUtils.isNumeric(num) && num.length<3){
      true
    }else{
      false
    }
  }

}
