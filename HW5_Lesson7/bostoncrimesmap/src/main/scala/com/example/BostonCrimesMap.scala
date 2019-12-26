package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BostonCrimesMap extends  App {

  val crime_file_path = "C:\\temp\\crimes-in-boston\\crime.csv"
  val offense_codes_file_path = "C:\\temp\\crimes-in-boston\\offense_codes.csv"
  val result_folder_path = "C:\\temp\\crimes-in-boston\\"

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  import spark.implicits._

  val crimes_df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(crime_file_path)
  val offense_codes_df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(offense_codes_file_path)

  val joined_df =  crimes_df.join(broadcast(offense_codes_df), crimes_df("OFFENSE_CODE") <=> offense_codes_df("CODE")).select("INCIDENT_NUMBER", "DISTRICT", "NAME", "YEAR", "MONTH", "Lat", "Long")

  // сформируем первый фрейм для которого достаточно только GROUP BY `DISTRICT`
  val df1 = joined_df
    .select($"DISTRICT", $"Lat", $"Long")
    .groupBy($"DISTRICT")
    .agg(
      count($"DISTRICT").as("TOTAL_CRIMES")
      , avg($"Lat").as("LATITUDE")
      , avg($"Long").as("LONGTITUDE")
    )
    .show()


}
