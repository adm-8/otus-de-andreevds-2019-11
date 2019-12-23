package com.github.adm8.de.hw4

import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class WinePosition(id: String, price: String, country: String, variety: String, title: String, winery: String )

object JsonReader extends App {

  // формруем контекст и читаем файл
  val spark = SparkSession.builder().master("local").getOrCreate()
  val sc = spark.sparkContext
  val filePath = args(0)
  val rddFile = sc.textFile(filePath)

  // пробегаемся по файлу
  rddFile.foreach( f = line => {

    // получаем данные
    val jsonLine = parse(line)

    val id = pretty(jsonLine \\ "id")
    val price = pretty(jsonLine \\ "price")
    val country = pretty(jsonLine \\ "country")
    val variety = pretty(jsonLine \\ "variety")
    val title = pretty(jsonLine \\ "title")
    val winery = pretty(jsonLine \\ "winery")

    // прокидываем в кейс-класс
    val wine = WinePosition(id, price, country, variety, title, winery)

    // печатаем
    println(wine)

  })




}

