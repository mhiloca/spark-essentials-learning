package part3_typesdatasets

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val session = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = session.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  import session.implicits._
  session.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  // Dates
  val dtFormats = Seq("dd-MMM-yy", "yyyy-MM-dd", "MMMM, yyyy")

  val moviesWithReleaseDates = moviesDF
    .select($"Title", coalesce(
      dtFormats.map(format => to_date($"Release_Date", format)):_*)
      .as("Actual_Release")
    ) // conversion
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn("Movie_age", datediff($"Today", $"Actual_Release") / 365) // date_add, date_sub

  moviesWithReleaseDates.select("*").where($"Actual_Release".isNull)/**/

  /**
    * Exercise
    * 1. How do we deal with multiple date formats? - coalesce?
    * 2. Read the stocks DF and parse the dates
    */

  val stocksDF = session.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  val stocksParsedDatesDF = stocksDF.withColumn("date", to_date($"date", "MMM dd yyyy"))

  //Structures
  // 1 - wit col operators
  moviesDF.select($"Title", struct($"US_Gross", $"Worldwide_Gross").as("Total_profit"))

  // 2 - with expression strings
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross as US_Profit")

  val moviesWithWords = moviesDF.select($"Title", split($"Title", " ").as("Title_Words"))

  moviesWithWords
    .select(
      $"Title",
      $"Title_Words",
      expr("Title_Words[0]"),
      size($"Title_Words"),
      array_contains($"Title_Words", "Love,")
    )
    .show()


}
