package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

// опишем кейс-классы для дата-фреймов
case class MainDF (DISTRICT:String, CRIMES_TOTAL:Double, LATITUDE:Double, LONGITUDE:Double) // Основной фрейм с простой группировкой
case class MedianDF (MDF_DISTRICT:String, CRIMES_MONTHLY:Double) // Фрейм с расчитанной медианой
case class CrimeTypeDF(ctf_DISTRICT:String ,ctf_CRIME_TYPE:String, count:Double, rn:Double) // Фрейм с расчётами типов преступлений

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


  // *****************************************************************
  // Формируем первый фрейм. Посчитаем те данные, для которых достаточно только GROUP BY `DISTRICT`
  // *****************************************************************

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



  // *****************************************************************
  // Формируем второй фрейм для подсчетов медианы - crimes_monthly
  // *****************************************************************

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



  // *****************************************************************
  // Формируем фрейм для результирующей колонки - frequent_crime_types
  // *****************************************************************

    // пишем кастомную функцию для обработки получения crime_type из NAME
    val f_get_crime_type = (str: String) => {
      str.split(" - ")(0)
    }
    spark.udf.register("f_get_crime_type", f_get_crime_type)

  // получаем сырые данные
  val df_crime_type_raw = joined_df
    .select("DISTRICT", "NAME")
    .createOrReplaceTempView("df_crime_type_raw")

  // преобразуем NAME в CRIME_TYPE и сделаем фрейм с топ 3 типами преступлений
  val windowSpec = Window.partitionBy("ctf_DISTRICT").orderBy($"count".desc)
  val df_crime_type_func = spark.sql("select DISTRICT as ctf_DISTRICT, f_get_crime_type(NAME) as ctf_CRIME_TYPE from df_crime_type_raw")
  val df_crime_type_top_3 = df_crime_type_func
    .select($"ctf_DISTRICT", $"ctf_CRIME_TYPE")
    .groupBy($"ctf_DISTRICT", $"ctf_CRIME_TYPE")
    .count()
    .withColumn("rn", row_number().over(windowSpec))
    .as[CrimeTypeDF]
    .groupByKey(x => x.ctf_DISTRICT)
    .flatMapGroups { // вероятнее всего есть вариант решения через mapGroups, но я в них не силен, поэтому пойду немного по другому пути
      case (districtKey, elements) => elements.toList.sortBy(x => x.rn).take(3)
    }

    // а теперь воспользуемся, возможно не самой подходящей для этого, window function LAG. Пользую её ибо пока не представляю как иначе перевести в записи "плоский вид"
    val windowSpec_lag = Window.partitionBy("ctf_DISTRICT").orderBy($"count")
    val df_crime_type_flat_3 = df_crime_type_top_3
      .select($"ctf_DISTRICT", $"ctf_CRIME_TYPE", $"count", $"rn")
      .withColumn("ctf_CRIME_TYPE_2", lag('ctf_CRIME_TYPE, 1) over windowSpec_lag)
      .withColumn("ctf_CRIME_TYPE_3", lag('ctf_CRIME_TYPE, 2) over windowSpec_lag)
      .where($"rn" === 1)
      .createOrReplaceTempView("df_crime_type_flat_3")

    // получаем результирующий фрейм по типам преступлений, готовый к ждоину с остальными фреймами
    val df_crime_type = spark.sql("select ctf_DISTRICT, concat(ctf_CRIME_TYPE, ', ', ctf_CRIME_TYPE_2, ', ', ctf_CRIME_TYPE_3) as FREQUENT_CRIME_TYPES from df_crime_type_flat_3 as t")



  // *****************************************************************
  // Формируем второй фрейм для подсчетов медианы
  // *****************************************************************

  val result_df = df_main
    .join(broadcast(df_CRIMES_MONTHLY), df_main("DISTRICT") <=> df_CRIMES_MONTHLY("MDF_DISTRICT"))
    .join(broadcast(df_crime_type), df_main("DISTRICT") <=> df_crime_type("ctf_DISTRICT"))
    .select("DISTRICT", "CRIMES_TOTAL", "CRIMES_MONTHLY" , "FREQUENT_CRIME_TYPES", "LATITUDE", "LONGITUDE")
    .show()
  // .write.csv(result_folder_path)




  //println("Result file will be written in " + result_file_path)
}


