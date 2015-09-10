package com.octo.nad.handson.domain

object Utils {
  def csvLineToArticle(line : Array[String]): Article ={
    val storeId = line.apply(0).toInt
    val product = line.apply(1)
    val section = line.apply(2)
    val price = line.apply(3).toInt / 100d
    val quantity = line.apply(4).toInt
    Article(storeId,product,section,price,quantity,0d)
  }
}
