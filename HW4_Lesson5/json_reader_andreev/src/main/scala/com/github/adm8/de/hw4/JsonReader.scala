package com.github.adm8.de.hw4

import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class WinePosition(id:Option[Int], points:Option[Int], price:Option[Double], country:Option[String], variety:Option[String], title:Option[String], winery:Option[String] ){
  def printThis(): Unit ={
    var str = "WinePosition("
    str += this.id.getOrElse(None).toString
    str += "," + this.points.getOrElse(None).toString
    str += "," + this.price.getOrElse(None).toString
    str += "," + this.country.getOrElse(None).toString
    str += "," + this.variety.getOrElse(None).toString
    str += "," + this.title.getOrElse(None).toString
    str += "," + this.winery.getOrElse(None).toString
    str += ")"
    println(str)
  }
}

object JsonReader extends App {

  // формруем контекст и читаем файл
  val spark = SparkSession.builder().master("local").getOrCreate()
  val sc = spark.sparkContext
  val filePath = args(0)
  //val filePath = "C:\\temp\\winemag-data-130k-v2.json"

  val rddFile = sc.textFile(filePath)

  // пробегаемся по файлу
  rddFile.foreach( f = line => {

    // получаем данные
    val jsonLine = parse(line)

    val id_str:Option[String] = Some(pretty(jsonLine \\ "id"))
    val price_str:Option[String] = Some(pretty(jsonLine \\ "price"))
    val points_str:Option[String] = Some(pretty(jsonLine \\ "points"))
    var country:Option[String] = Some(pretty(jsonLine \\ "country"))
    var variety:Option[String] = Some(pretty(jsonLine \\ "variety"))
    var title:Option[String] = Some(pretty(jsonLine \\ "title"))
    var winery:Option[String] = Some(pretty(jsonLine \\ "winery"))

    val id:Option[Int] = if (id_str.get == "{ }") None else Some(id_str.get.toInt)
    val points:Option[Int] = if (points_str.get == "{ }") None else Some(points_str.get.toInt)
    val price:Option[Double] = if (price_str.get == "{ }") None else Some(price_str.get.toDouble)
    country = if (country.get == "{ }") None else  Some(country.get)
    variety = if (variety.get == "{ }") None else  Some(variety.get)
    title = if (title.get == "{ }") None else  Some(title.get)
    winery = if (winery.get == "{ }") None else  Some(winery.get)


    // прокидываем в кейс-класс
    WinePosition(id, points, price, country, variety, title, winery).printThis()


  })


}

