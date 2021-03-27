package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
object SparkSql extends App {

  val session = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDF = session.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  import session.implicits._

  // regular DF API
  carsDF.select($"Name").where($"Origin" === "USA")

  // use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = session.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin
  )
//  americanCarsDF.show(false)

  session.sql("create database rtjvm")

  val databaseDF = session.sql("show databases")
  databaseDF.show()

  // transfer tables from a DB to spark tables
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

  def transferTables(tableNames: List[String]) = {
    tableNames.foreach { tableName =>
      val tableDF = readTable(tableName)

      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

  val tableNames = List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager"
  )

  // we can run ANY SQL statement
  session.sql("use rtjvm")

  transferTables(tableNames)
  session.sql("show tables")

  // read DF from warehouse
  def createDF(tableName: String) = session.read.table(tableName)

  val employeesDF = createDF("employees")
  val departmentsDF = createDF("departments")
  val titlesDF = createDF("titles")
  val deptEmployeesDF = createDF("dept_emp")
  val salariesDF = createDF("salaries")
  val deptManagersDF = createDF("dept_manager")

  /**
    * Exercises
    *
    * 1. Read the movies DF and sore it as a SparkTable in the rtjvm database
    * 2. Count how many employees were hired in between 1 1999 and Jan 1 2001
    * 3. Show the average salaries for the employees hired in between those dates, grouped by department
    * 4. Show the name of the best-paying department for employees hired in between those dates
    */

//  session.sql(
//    """
//      |select *
//      |from employees e, dept_emp d
//      |where e.emp_no = d.emp_no
//      |""".stripMargin
//  )

  // 1. moviesDF as a spark table
  val moviesDF = session.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("movies")

  // 2. Count how many employees hired 1 Jan 1999 and 1 Jan 2000
  val employeesHiredBetweenDatesDF = employeesDF
    .withColumn("hire_date", to_date($"hire_date","yyyy-MM-dd"))
    .where($"hire_date".between("1999-01-01", "2000-01-01"))

  println(employeesHiredBetweenDatesDF.count())

  session.sql(
    """
      |select count(*)
      |from employees
      |where hire_date > '1999-01-01' and hire_date < '2000-01-01'
      |""".stripMargin
  ).show()

  // 3.
  val avgSalariesGroupedByDepartmentDF = employeesHiredBetweenDatesDF
    .join(
      deptEmployeesDF,
      employeesHiredBetweenDatesDF.col("emp_no") === deptEmployeesDF.col("emp_no")
    )
    .drop(deptEmployeesDF.col("emp_no"))
    .join(
      departmentsDF,
      deptEmployeesDF.col("dept_no") === departmentsDF.col("dept_no")
    )
    .drop(departmentsDF.col("dept_no"))
    .join(
      salariesDF,
      salariesDF.col("emp_no") === employeesHiredBetweenDatesDF.col("emp_no")
    )
    .groupBy("dept_name")
    .agg(avg("salary").as("avg_salary"))

  session.sql(
    """
      |select de.dept_no , avg(s.salary)
      |from employees e, dept_emp de, salaries s
      |where e.hire_date < '1999-01-01' and e.hire_date < '2000-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      |group by de.dept_no
      |""".stripMargin
  ).show()

  avgSalariesGroupedByDepartmentDF.show()

  val bestPayingDepartment = avgSalariesGroupedByDepartmentDF
    .groupBy($"dept_name")
    .agg(max("avg_salary"))
  bestPayingDepartment.show()

  session.sql(
    """
      |select d.dept_name, avg(s.salary) payments
      |from employees e, dept_emp de, salaries s, departments d
      |where e.hire_date < '1999-01-01' and e.hire_date < '2000-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      | and de.dept_no = d.dept_no
      |group by d.dept_name
      |order by payments desc
      |limit 1
      |""".stripMargin
  ).show()


}
