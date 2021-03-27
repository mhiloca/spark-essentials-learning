package part2_dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsExpressions extends App {


  val session = SparkSession.builder()
    .appName("Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = session.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

//  carsDF.show()

  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting (projection) - we are projecting the df into a new df with fewer columns
  val carsNamesDf = carsDF.select(firstColumn)

  // various select methods
  import session.implicits._

  carsDF.select(
    col("Name"),
    column("Acceleration"),
    'Year, // Scala Symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin") // EXPRESSION
  )

  // selection with col names
  carsDF.select("Name", "Year")

  //not: these methods are not inter-exchangeable you must chose a way, either names or functions and expressions

   // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg2")
  )

  // selectExpr
  val carsWithSelectExprWeightDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing

  // adding a column
  val carsWithKgDF3 = carsDF.withColumn("Weight_in_kg3", col("Weight_in_lbs") / 2.2)

  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")

  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val americanCarsDF2 = carsDF.where(col("Origin") === "USA")

  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")

  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(($"Origin" === "USA").and($"Horsepower" > 150))
  val americanPowerfulCarsDF2 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // unioning = adding more rows
  val moreCarsDF = session.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")

  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
//  allCountriesDF.show()

  /**
    * Exercises
    *
    * 1. Read movies DF and select two other columns - title, and release date
    * 2. create a df another column summing up the total profir of the movies = US_Gross + Worldwide_Gross
    * 3. Select all good comedies with IMDB rating above 6
    *
    * use as many versions as possible
    */

  val moviesDF = session.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // 1. Select two other columns
  val titleReleaseDateMoviesDF = moviesDF.selectExpr("Title", "Release_date")
  val titleReleaseDateMoviesDF2 = moviesDF.select($"Title", $"Release_date")
  val titleReleaseDateMoviesDF3 = moviesDF.select('Title, 'Release_date)

  titleReleaseDateMoviesDF2.show()

  // 2. Create a DF with total profit column
  val moviesWithTotalProfitDF = moviesDF.withColumn(
    "Total_profit",
    $"US_Gross" + $"Worldwide_Gross"
  )

  val moviesWithTotalProfitDF2 = moviesDF.select(
    $"Title",
    $"US_Gross",
    $"Worldwide_Gross",
    $"US_DVD_Sales",
    ($"US_Gross" + $"Worldwide_Gross").as("Total_profit")
  )

  moviesWithTotalProfitDF2.show()
//  moviesWithTotalProfitDF.show()

  // 3. Filter good comedies
  val goodComediesMoviesDF = moviesDF.filter($"Major_Genre" === "Comedy" and $"IMDB_Rating" > 6)
  goodComediesMoviesDF.show()

  val atLeastMediocreComedyDF = moviesDF.select($"Title", $"IMDB_Rating")
    .where($"Major_Genre" === "Comedy" and $"IMDB_Rating" > 6)

}
