package playground

import org.apache.spark.sql._
import org.apache.log4j.{Logger, Level}

object CarsCatalog extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder()
    .appName("CarsCatalog")
    .master("local[*]")
    .getOrCreate()

  case class Cars(
                   Name: String,
                   Miles_per_Gallon: Double,
                   Cylinders: Long,
                   Displacement: Double,
                   Horsepower: Long,
                   Weight_in_Lbs: Long,
                   Acceleration: Double,
                   Year: String,
                   Origin: String
                 )

  import session.implicits._

  val cars = session
    .read
    .parquet("src/main/resources/data/cars.parquet")

  val carsColumns = cars.columns.mkString(", ")

  val carsDS = cars.selectExpr("*").as[Cars]

//  carsDS
//    .write
//    .mode(SaveMode.Overwrite)
//    .option("compression", "gzip")
//    .save("src/main/resources/data/test")

  val test = session
    .read
    .parquet("src/main/resources/data/test")

  val listPaths = Seq(
    "src/main/resources/data/cars.parquet",
    "src/main/resources/data/test"
  )

  val testDS = session.read.parquet(listPaths:_*).selectExpr("*").as[Cars]

  testDS.show(testDS.count.toInt)

}
