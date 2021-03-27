package part3_typesdatasets

import org.apache.spark.sql.functions.{array_contains, avg}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}


object Datasets extends App {

  val session = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = session.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  // convert a DF to a dataset
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  numbersDS.filter(_ < 100)

  // dataset of a complex type
  // 1 - define your case class
  case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double],
                Cylinders: Option[Long],
                Displacement: Option[Double],
                Horsepower: Option[Long],
                Weight_in_lbs: Option[Long],
                Acceleration: Option[Double],
                Year: String,
                Origin: String
                )

  // 2 - read the dataframe from the file
  def readDF(fileName: String) = session.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$fileName")

  // 3 - define an encoder (importing the implicits)
  import session.implicits._
  val carsDF = readDF("cars.json")
  val carsDS = carsDF.as[Car]

  // DS collection functions
  numbersDS.filter(_ < 100)

  val carsWithUpperCase = carsDS.map( car => car.Name.toUpperCase())

  /**
    * Exercises
    *
    * 1. Count how many cars we have
    * 2. Count how many POWERFUL cars we have(HP >140)
    * 3. Average HP fo the entire dataset
    */

/*  val carsCount = carsDS.count
  println(carsCount)
  println(carsDS.filter(car => car.Horsepower.getOrElse(0L) > 140).count)
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount)*/

//  carsDS.select(avg("Horsepower")).show()

  // Joins
  case class Guitar(id: Long, model: String, make: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS
    .joinWith(
      bandsDS,
      guitarPlayersDS.col("band") === bandsDS.col("id"), "inner"
    ) // inner by default

//  guitarPlayerBandDS.show(false)

  /**
    * Exercise: join guitarsDS and guitarPlayersDS
    * (hint: use array_contains)
    */

  val guitarsAndGuitarPlayersDS = guitarPlayersDS
    .joinWith(
      guitarsDS,
      array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")),
      "outer"
    )

//  guitarsAndGuitarPlayersDS.show(false)

  // Grouping
  val carsGroupedByOrigin = carsDS
    .groupByKey(_.Origin)
    .count()
    .show()

  // joins and groups are WIDE transformations, will involve SHUFFLE operations
  // changes the number of partitions
}


