package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// опишем кейс-классы для дата-фреймов
case class MainDF (district:String, crimes_total:Double, lat:Double, lng:Double) // Основной фрейм с простой группировкой
case class MedianDF (MDF_district:String, crimes_monthly:Double) // Фрейм с расчитанной медианой
case class CrimeTypeDF(ctf_district:String ,ctf_CRIME_TYPE:String, count:Double, rn:Double) // Фрейм с расчётами типов преступлений

object BostonCrimesMap extends  App {

  // получение путей к файлам
/*
  val crime_file_path = "C:\\temp\\crimes-in-boston\\crime.csv"
  val offense_codes_file_path = "C:\\temp\\crimes-in-boston\\offense_codes.csv"
  val result_folder_path = "C:\\temp\\crimes-in-boston\\result"
*/

  val crime_file_path = args(0)
  val offense_codes_file_path = args(1)
  val result_folder_path = args(2)

  // Создаем спраковую сессию
  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  import spark.implicits._

  // читаем данные из файлов и создаем на их основе дата фреймы
  val crimes_df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(crime_file_path)
  val offense_codes_df_raw = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(offense_codes_file_path)


  // - - - - - - - - - -

  val crimes_df_view = crimes_df.createOrReplaceTempView("crimes_df_view")
  val offense_codes_df_view = offense_codes_df_raw.createOrReplaceTempView("offense_codes_df_view")

  val sql = "select code, name from (select code, name, row_number() over (partition by code order by name) as rn from offense_codes_df_view) where rn = 1"
  val offense_codes_df = spark.sql(sql)
  val offense_codes_df_fixed_view = offense_codes_df.createOrReplaceTempView("offense_codes_df_fixed_view")

  //println("Lest see our raw crimes_df_view data!")
  //spark.sql("select count(*) from crimes_df_view").show() // 319073
  //spark.sql("select count(*) from crimes_df_view t1 left outer join offense_codes_df_fixed_view t2 on t1.OFFENSE_CODE = t2.CODE").show() // 577880
  //val sql = "select * from offense_codes_df_view where code in (select code from offense_codes_df_view group by code having count(*) > 1) order by code"


  // - - - - - - - - - -

  // джоиним фреймы и выбираем те колонки, которые понадобятся нам для выполнения ДЗ
  val joined_df =  crimes_df.join(broadcast(offense_codes_df), crimes_df("OFFENSE_CODE") <=> offense_codes_df("CODE")).select("INCIDENT_NUMBER", "district", "NAME", "YEAR", "MONTH", "Lat", "Long")








  // *****************************************************************
  // Формируем первый фрейм. Посчитаем те данные, для которых достаточно только GROUP BY `district`
  // *****************************************************************

  val df_main = joined_df
    .select($"district", $"Lat", $"Long")
    .groupBy($"district")
    .agg(
      count($"*").as("crimes_total")
      , avg($"Lat").as("lat")
      , avg($"Long").as("lng")
    )
    .as[MainDF] // ссылаемся на описаный ранее класс
    //.alias("df_main")
    //.show()



  // *****************************************************************
  // Формируем второй фрейм для подсчетов медианы - crimes_monthly
  // *****************************************************************

  // подготовим вьюху для расчёта медианы.
    val df_crimes_year_monthly_count = joined_df
      .select($"district", $"YEAR", $"MONTH")
      .groupBy($"district", $"YEAR", $"MONTH")
      .count()
      .createOrReplaceTempView("df_crimes_year_monthly_count")

    // посчитаем медиану
    val df_crimes_monthly = spark.sql("SELECT district as MDF_district, percentile_approx(count, 0.5) as crimes_monthly FROM df_crimes_year_monthly_count group by district")
      .as[MedianDF] // ссылаемся на описаный ранее класс
    //.alias("df_crimes_monthly")
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
    .select("district", "NAME")
    .createOrReplaceTempView("df_crime_type_raw")

  // преобразуем NAME в CRIME_TYPE и сделаем фрейм с топ 3 типами преступлений
  val windowSpec = Window.partitionBy("ctf_district").orderBy($"count".desc)
  val df_crime_type_func = spark.sql("select district as ctf_district, f_get_crime_type(NAME) as ctf_CRIME_TYPE from df_crime_type_raw")
  val df_crime_type_top_3 = df_crime_type_func
    .select($"ctf_district", $"ctf_CRIME_TYPE")
    .groupBy($"ctf_district", $"ctf_CRIME_TYPE")
    .count()
    .withColumn("rn", row_number().over(windowSpec))
    .as[CrimeTypeDF]
    .groupByKey(x => x.ctf_district)
    .flatMapGroups { // вероятнее всего есть вариант решения через mapGroups, но я в них не силен, поэтому пойду немного по другому пути
      case (districtKey, elements) => elements.toList.sortBy(x => x.rn).take(3)
    }

    // а теперь воспользуемся, возможно не самой подходящей для этого, window function LAG. Пользую её ибо пока не представляю как иначе перевести в записи "плоский вид"
    val windowSpec_lag = Window.partitionBy("ctf_district").orderBy($"count")
    val df_crime_type_flat_3 = df_crime_type_top_3
      .select($"ctf_district", $"ctf_CRIME_TYPE", $"count", $"rn")
      .withColumn("ctf_CRIME_TYPE_2", lag('ctf_CRIME_TYPE, 1) over windowSpec_lag)
      .withColumn("ctf_CRIME_TYPE_3", lag('ctf_CRIME_TYPE, 2) over windowSpec_lag)
      .where($"rn" === 1)
      .createOrReplaceTempView("df_crime_type_flat_3")

    // получаем результирующий фрейм по типам преступлений, готовый к д;оину с остальными фреймами
    val df_crime_type = spark.sql("select ctf_district, concat(ctf_CRIME_TYPE, ', ', ctf_CRIME_TYPE_2, ', ', ctf_CRIME_TYPE_3) as frequent_crime_types from df_crime_type_flat_3 as t")



  // *****************************************************************
  // Формируем результирующий фрейм и пишем его в файл
  // *****************************************************************

  val result_df = df_main
    .join(broadcast(df_crimes_monthly), df_main("district") <=> df_crimes_monthly("MDF_district"))
    .join(broadcast(df_crime_type), df_main("district") <=> df_crime_type("ctf_district"))
    .select("district", "crimes_total", "crimes_monthly" , "frequent_crime_types", "lat", "lng")
    .coalesce(1).write.parquet(result_folder_path)
    //.show()


}


