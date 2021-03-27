package part5_rdd

import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

import scala.io.Source

object RDDs extends App {

  val session = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = session.sparkContext

  // 1 - parallelize an existing collection
  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)

  // 2 - reading from files
  case class StockValue(symbol: String, date: String, price: Double)

  def readStocks(filename: String) = {
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  }

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b - reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
  val stocksDF = session.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  val stocksRDD4 = stocksDF.rdd // you get the RDD[Row]

  import session.implicits._

  val stocksDS = stocksDF.as[StockValue]
  val stockRDD3 = stocksDS.rdd

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers") // needs to pass the column names and you loose the type information

  // RDD -> DS
  val numbersDS = session.createDataset(numbersRDD)

  // transformations
  val msftRDD = stocksRDD2.filter(_.symbol == "MSFT") // lazy transformation
  val msCount = msftRDD.count() // eager ACTION

  val symbolNamesRDD = stocksRDD.map(_.symbol).distinct() // lazy transformation

  // min and max
  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan[StockValue]((a: StockValue, b: StockValue) => a.price < b.price)
  val minMsft = msftRDD.min()

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol) // very expensive

  // Partitioning

  //  val repartitionedStocksRDD = stocksRDD.repartition(30)
  //  repartitionedStocksRDD.toDF
  //    .write.mode(SaveMode.Overwrite)
  //    .parquet("src/main/resources/data/stocks30")
  //
  //  /*
  //    Repartitioning is EXPENSIVE - involves shuffling
  //    best practice: partition EARLY, then process that
  //    size of a partition should be between 10 and 100MB.
  //   */
  //
  //  // coalesce
  //  val coalesceRDD = repartitionedStocksRDD.coalesce(15) // will not involve shuffling
  //  coalesceRDD.toDF.write
  //    .mode(SaveMode.Overwrite)
  //    .parquet("src/main/resources/data/stocks15")

  /**
    * Exercises
    *
    * 1. read the movies.json as RDD
    * 2. show the distinct genres as RDD
    * 3. select all the movies in the Drama genre with IMDB rating > 6
    * 4. show the average rating of movies by genre
    */

  case class Movie(title: String, genre: String, rating: Double)

  // 1
  import session.implicits._
  val moviesRDD = session.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
    .select($"Title".as("title"), $"Major_Genre".as("genre"), $"IMDB_Rating".as("rating"))
    .na.fill(Map("rating" -> 0.0, "genre" -> "unknown"))
    .as[Movie]
    .rdd

  val distinctGenresRDD = moviesRDD.map(_.genre).distinct()
  distinctGenresRDD.toDF.show()

  val mediocreMovies = moviesRDD.filter(movie => movie.rating > 6 && movie.genre == "Drama")
  mediocreMovies.toDF.show()

  // 1 way
  val averageByGenre = moviesRDD
    .groupBy(_.genre)
    .map{ row =>
      val ratings = row._2.map(_.rating)
      val avgRating = ratings.sum / row._2.size.toDouble
      (row._1, avgRating)
    }

  // 2 way
  case class GenreAvgRating(genre: String, rating: Double)

  val avgRatingByGenre = moviesRDD
    .groupBy(_.genre).map {
    case (genre, movies) =>
      GenreAvgRating(genre, movies.map(_.rating).sum / movies.size.toDouble)
  }

  avgRatingByGenre.toDF.show()

}