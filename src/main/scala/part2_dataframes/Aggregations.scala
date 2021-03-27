package part2_dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, avg, count, countDistinct, mean, min, stddev, sum}

object Aggregations extends App {

  val session = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  import session.implicits._
  val moviesDF = session.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // counting
  val genresCountDF = moviesDF.select(count($"Major_Genre")) // all the values except null
//  moviesDF.select(count("*")).show()

  //counting distinct values
//  moviesDF.select(countDistinct($"Major_Genre")).show()

  // approximate version
  moviesDF.select(approx_count_distinct($"Major_Genre"))

  // min and max
//  val minRatingDF = moviesDF.select(min($"IMDB_Rating")).show()

  // sum and avg
  moviesDF.select(sum($"US_Gross"))
  moviesDF.selectExpr("sum(US_Gross)")

  moviesDF.select(avg($"Rotten_Tomatoes_Rating"))
  moviesDF.selectExpr("avg(Rotten_tomatoes_Rating)")

  // data science
//  moviesDF.select(
//    mean($"Rotten_Tomatoes_Rating"),
//    stddev($"Rotten_Tomatoes_Rating")
//  ).show()

  // Grouping
  val countByGenreDF = moviesDF.groupBy($"Major_Genre") // also include null
    .count() // select count(*) from moviesDF group by Major_Genre

  val avgRatingByGenre = moviesDF
    .groupBy($"Major_Genre")
    .avg("IMDB_Rating")

  val aggregationsByGenre = moviesDF
    .groupBy($"Major_Genre")
    .agg(
      count("*").as("All_movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy("Avg_Rating")

//  aggregationsByGenre.show()


  /**
    * Exercises
    *
    * 1. Sum up ALL the profits ALL the movies in the DF
    * 2. Count how many distinct directors we have
    * 3. Show the mean and standard deviation of US gross revenue fo the movies
    * 4. Compute the average IMDB rating and the average US gross revenue per Director
    */

  // 1. Sum up the profit
  val totalProfitMoviesDF = moviesDF
    .select(
      sum($"US_Gross" + $"Worldwide_Gross")
        .as("Total_Profit")
    ).show()

  // 2. Count how many distinct directors we have
  moviesDF.select(countDistinct("Director"))

  // 3. Show the mean and standard deviation of US gross revenue for the movies
  moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  ).show()

  // 4. Compute the average IMDB rating and the average US gross revenue per Director
  moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating")
      .as("Avg_Rating"),
      avg("US_Gross")
        .as("Avg_Gross")
    )
    .orderBy($"Avg_Rating".desc)
    .show()

}
