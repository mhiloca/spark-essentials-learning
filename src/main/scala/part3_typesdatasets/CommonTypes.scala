package part3_typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

object CommonTypes extends App {

  val session = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = session.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value to a DF

  import session.implicits._
  moviesDF.select($"Title", lit(47).as("plain_value")) // lit will add a value

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter
  moviesDF.select($"Title").where(preferredFilter)

  val moviesWithGoodnessFlagDF = moviesDF.select($"Title", preferredFilter.as("good_movie"))

  // filter on a boolean column
  moviesWithGoodnessFlagDF.where("good_movie") // where($"good_movie" === "true")

  // negations
  moviesWithGoodnessFlagDF.where(not($"good_movie"))

  // Numbers
  // math operators
  val moviesAvgRatingsDF = moviesDF.select($"Title", $"Rotten_Tomatoes_Rating" / 10 + $"IMDB_Rating" / 2)

  // correlation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) /* corr is an ACTION */

  // Strings
  val carsDF = session.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // capitalization: initcap, lower, upper
  carsDF.select(initcap($"Name")).show()

  // contains
  carsDF.select("*").where($"Name".contains("Volkswagen"))

  // regex
  val regexString = "volksvagen|vw"
  val vwDF  = carsDF.select(
    $"Name",
    regexp_extract($"Name", regexString, 0).as("regex_extract")
  ).where($"regex_extract" =!= "")

  vwDF.select(
    $"Name",
    regexp_replace($"Name", regexString, "People's Car").as("regex_replace")
  ).show()

  /**
    * Exercise
    *
    * Filter the cars DF by a list of car names obtained by an API
    * Versions:
    *  - contains
    *  - regex
    *
    */

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  val carsRegex = getCarNames.map(_.toLowerCase).mkString("|")

  carsDF
    .select($"*", regexp_extract($"Name", carsRegex, 0).as("regex_extract"))
    .where($"regex_extract" =!= "")
    .drop("regex_extract")
    .show()

  val carNameFilters = getCarNames.map(_.toLowerCase).map(name => $"Name".contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show()

}
