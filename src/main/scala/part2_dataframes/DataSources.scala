package part2_dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App {

  val session = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType, nullable = true),  // describe columns
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_Lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
    Reading a DF:
      - format
      - schema or .option("inferSchema", "true")
      - zero or more options
      -path
   */
  val carsDF = session.read
    .format("json")
    .schema(carsSchema) // enforce a schema
    .option("mode", "failFast") // dropMalformed, permissive (default)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  // alternative reading with options map
  val carsDFWithOptionMap = session.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /*
   writing DFs
    - format
    - save mode = overWrite, append, ignore, errorExists
    - path
    - zero or more options
   */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")

  // JSON flags
  session.read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // couple with schema, if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") //bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price",DoubleType)
  ))

  session.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "") // this instructs spark to parse "" into null in the DF
    .csv("src/main/resources/data/stocks.csv")

  // Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")

  //Text files
  session.read.text("src/main/resources/data/sampleTextFile.txt").show()

//   Reading from a remote DB
  val employeesDF = session.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

  /**
    * Exercise: read the movies DF and write it as:
    *  - csv: tab-seperated values file
    *  - snappy Parquet
    *  - table public.movies in the Postgres DB
    *
    */

  val moviesDf = session.read
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-YY")
    .json("src/main/resources/data/movies.json")

  moviesDf.write
    .option("header", "true")
    .option("sep", "\t")
    .mode(SaveMode.Overwrite)
    .csv("src/main/resources/data/movies.csv")

  moviesDf.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies.parquet") // it is already compressed snappy

  moviesDf.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .mode(SaveMode.Overwrite)
    .save()

}
