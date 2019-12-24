package com.github.adm8.de.hw4

import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class WinePosition(id: Int, points: Int, price: Double, country: String, variety: String, title: String, winery: String )

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

    val id_str = pretty(jsonLine \\ "id")
    val price_str = pretty(jsonLine \\ "price")
    val points_str = pretty(jsonLine \\ "points")
    var country = pretty(jsonLine \\ "country")
    var variety = pretty(jsonLine \\ "variety")
    var title = pretty(jsonLine \\ "title")
    var winery = pretty(jsonLine \\ "winery")

    val id = if (id_str == "{ }") 0 else id_str.toInt
    val points = if (points_str == "{ }") 0 else points_str.toInt
    val price = if (price_str == "{ }") 0 else price_str.toDouble
    country = if (country == "{ }") "" else  country
    variety = if (variety == "{ }") "" else  variety
    title = if (title == "{ }") "" else  title
    winery = if (winery == "{ }") "" else  winery

    // прокидываем в кейс-класс
    val wine = WinePosition(id, points, price, country, variety, title, winery)
    // печатаем
    println(wine)



  })




}

