package part2_dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFramesBasics extends App {

  // creating a SparkSession
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  // reading a DF
   val firstDF = spark.read
     .format("json") // read a json file
     .option("inferSchema", "true") // all the columns are going to be figured out from the json structure
     .load("src/main/resources/data/cars.json") // path to the file

  // showing a DF
  firstDF.show()
  firstDF.printSchema()

  // get rows
  firstDF.take(10).foreach(println) // you get an array of rows

  // spark types
  val longType = LongType

  // schema
  val carsSchema = StructType(Array(
      StructField("Name", StringType, nullable = true),  // describe columns
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_Lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
  ))

  // obtain a schema
  val carsDFSchema = firstDF.schema

  println(carsDFSchema)

  // read a DF with your schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  // create rows by hand
  // rows can contain anything
  val myRow = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  // create DF from tuples

  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred

  // note: schemas are only applicable to dfs not to rows

  // create DFs with implicits
  import spark.implicits._
  val manualCarsWithImplicits = cars.toDF(
    "Name",
    "MPG",
    "Cylinders",
    "Displacement",
    "Horsepower",
    "Weight_in_Lbs",
    "Acceleration",
    "Year",
    "Origin"
  )

  manualCarsDF.printSchema()
  manualCarsWithImplicits.printSchema()

  /**
    * Exercises:
    * 1) Create a manual DF describing smartphones
    *     - make
    *     - model
    *     - screen dimension
    *     - camera megapixels
    *
    * 2) Read another file from the data folder, e.g. movies.json
    *     - print its schema
    *     - count the number of rows: count()
    */

  val smartphones = Seq(
    ("samsung", "galaxy1", 7.0, 21),
    ("motorola", "AK3", 6.9, 18),
    ("samsung", "galaxy9", 5.5, 21),
    ("lg", "g5", 7.0, 24),
    ("xaomi", "new3", 6.9, 13),
    ("lg", "g4", 5.5, 21),
    ("samsung", "galaxyS", 7.0, 24),
  )

  // import spark.implicits._
  val smartphonesWithSchema = smartphones.toDF("Make", "Model", "Screen_Dimensions", "Camera_MP")

  val smartphonesDF = spark.createDataFrame(smartphones)

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true") // input your own schema in production
    .load("src/main/resources/data/movies.json")

  smartphonesDF.printSchema()
  smartphonesWithSchema.printSchema()
  moviesDF.printSchema()

  println(moviesDF.count()) // comp0utes the number of rows in a DF

}
