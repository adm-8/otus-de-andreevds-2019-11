package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

// опишем кейс-классы для дата-фреймов
case class MainDF (DISTRICT:String, CRIMES_TOTAL:Double, LATITUDE:Double, LONGITUDE:Double) // Основной фрейм с простой группировкой
case class MedianDF (MDF_DISTRICT:String, CRIMES_MONTHLY:Double) // Фрейм с расчитанной медианой

object BostonCrimesMap extends  App {

  // получение путей к файлам
  val crime_file_path = "C:\\temp\\crimes-in-boston\\crime.csv"
  val offense_codes_file_path = "C:\\temp\\crimes-in-boston\\offense_codes.csv"
  val result_folder_path = "C:\\temp\\crimes-in-boston\\result"




  // Создаем спраковую сессию
  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  import spark.implicits._

  // читаем данные из файлов и создаем на их основе дата фреймы
  val crimes_df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(crime_file_path)
  val offense_codes_df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(offense_codes_file_path)

  // джоиним фреймы и выбираем те колонки, которые понадобятся нам для выполнения ДЗ
  val joined_df =  crimes_df.join(broadcast(offense_codes_df), crimes_df("OFFENSE_CODE") <=> offense_codes_df("CODE")).select("INCIDENT_NUMBER", "DISTRICT", "NAME", "YEAR", "MONTH", "Lat", "Long")

  // сформируем первый фрейм. Посчитаем те данные, для которых достаточно только GROUP BY `DISTRICT`
  val df_main = joined_df
    .select($"DISTRICT", $"Lat", $"Long")
    .groupBy($"DISTRICT")
    .agg(
      count($"*").as("CRIMES_TOTAL")
      , avg($"Lat").as("LATITUDE")
      , avg($"Long").as("LONGITUDE")
    )
    .as[MainDF] // ссылаемся на описаный ранее класс
    //.alias("df_main")
    //.show()

    // подготовим вьюху для расчёта медианы.
    val df_crimes_year_monthly_count = joined_df
      .select($"DISTRICT", $"YEAR", $"MONTH")
      .groupBy($"DISTRICT", $"YEAR", $"MONTH")
      .count()
      .createOrReplaceTempView("df_crimes_year_monthly_count")

    // посчитаем медиану
    val df_CRIMES_MONTHLY = spark.sql("SELECT DISTRICT as MDF_DISTRICT, percentile_approx(count, 0.5) as CRIMES_MONTHLY FROM df_crimes_year_monthly_count group by DISTRICT")
      .as[MedianDF] // ссылаемся на описаный ранее класс
    //.alias("df_CRIMES_MONTHLY")
    //.show()


    // Соберем результирующую таблицу
    val result_df = df_main.join(broadcast(df_CRIMES_MONTHLY), df_main("DISTRICT") <=> df_CRIMES_MONTHLY("MDF_DISTRICT"))
    //.select("DISTRICT", "CRIMES_TOTAL", "CRIMES_MONTHLY" , "LATITUDE", "LONGITUDE")
      .select("*")
      .show()
    // .write.csv(result_folder_path)


    //println("Result file will be written in " + result_file_path)
}
