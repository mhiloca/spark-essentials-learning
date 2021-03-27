package part2_dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{first, max, to_date}

object Joins extends App {

  val session = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  import session.implicits._

  val guitarsDF = session.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitarPlayersDF = session.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = session.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // inner join
  val joinCondition = guitarPlayersDF.col("band") === bandsDF.col("id")
  val guitaristBandsDF = guitarPlayersDF.join(bandsDF, joinCondition, "inner") // default value

  // outer join
  // left outer = everything in the innner join + all the rows in the LEFT table, with nulls where data is missing
  guitarPlayersDF.join(bandsDF, joinCondition, "left_outer")

  // right outer = everything in the innner join + all the rows in the RIGHT table, with nulls where data is missing
  guitarPlayersDF.join(bandsDF, joinCondition, "right_outer")

  // outer join = everything in the inner join + all the row in BOTH tables
  guitarPlayersDF.join(bandsDF, joinCondition, "outer")

  // semi joins = inner join - the info from the RIGHT table
  guitarPlayersDF.join(bandsDF, joinCondition, "leftsemi")

  // anti-join = everything in the left DF for which there is NO row in the right DF satisfying the condition
  guitarPlayersDF.join(bandsDF, joinCondition, "left_anti")

//  guitaristBandsDF.select("id", "band")

  // option 1 - rename the column on which we are joining
  guitarPlayersDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitaristBandsDF.drop(bandsDF.col("id"))

  // option 3 - rename the offending column and keep the data (we still have dupe data)
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitarPlayersDF.join(bandsDF, guitarPlayersDF.col("band") === bandsDF.col("bandId"))

  // use complex types
//  guitarPlayersDF.join(
//    guitarsDF.withColumnRenamed("id", "guitarId"),
//    expr("array_contains(guitars, guitarId)")
//  )

  /**
    * Exercises
    *
    * 1. show all the employees and their max salaries
    * 2. show all the employees who were never managers
    * 3. find the job title of the best paid 10 employees
    *
    */


  val jdbc = "jdbc"
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = session.read
    .format(jdbc)
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF = readTable("employees")
  val titlesDF = readTable("titles")
  val deptManagersDF = readTable("dept_manager")
  val salariesDF = readTable("salaries")

  // 1. ALL employees and their max salaries
  val maxSalariesPerEmpNoDF = salariesDF.groupBy($"emp_no").agg(max("salary").as("max_salary"))
  val employeesSalariesDF = employeesDF
    .join(maxSalariesPerEmpNoDF, "emp_no")
    .drop(maxSalariesPerEmpNoDF.col("emp_no"))



  // 2. ALL employees who were never managers
  val employeesNeverManagersDF = employeesDF
    .join(
      deptManagersDF,
      employeesDF.col("emp_no") === deptManagersDF.col("emp_no"),
      "left_anti"
    )


  // 3. find the job title of the best ten paid employees
  val tenBestPaidEmployeesTitles = employeesSalariesDF
    .join(
      titlesDF,
      employeesSalariesDF.col("emp_no") === titlesDF.col("emp_no")
    )
    .drop(employeesSalariesDF.col("emp_no"))
    .groupBy("emp_no")
    .agg(
      max("to_date") as "to_date",
      first("title") as "title",
      max("max_salary") as "max_salary"
    )
    .orderBy($"max_salary".desc)
    .limit(10)

  tenBestPaidEmployeesTitles.show()
}
