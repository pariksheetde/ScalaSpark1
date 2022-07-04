package Data_Engineering_2

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, max, mean, min, round, sum}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.count

object Loc_Dept_Emp_Analysis_1 {
  println("Locations wise Employees Analysis on Departments")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Locations wise Employees Analysis on Departments")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    //    .enableHiveSupport()
    .getOrCreate()

  //  read the departments datafile from the location
  val dept_df = spark.read
    .format("csv")
    .options(Map(
      "header" -> "true",
      "mode" -> "PERMISSIVE",
      "path" -> "D:/DataSet/DataSet/SparkDataSet/departments.csv",
      "inferSchema" -> "true",
      "nullValue"-> "NA",
      "sep" -> ",",
      "compression" -> "snappy", // bzip2, gzip, lz4, deflate, uncompressed
      "dateFormat" -> "dd/MM/yyyy",
      "allowSingleQuotes" -> "true"
    ))
    .load()

  //  convert the upper case column to lower case
  val cln_dept_df = dept_df.selectExpr("Dept_ID", "Dept_Name", "Loc_ID")


  //  read the locations datafile from the location
  val loc_df = spark.read
    .format("csv")
    .options(Map(
      "header" -> "true",
      "mode" -> "PERMISSIVE",
      "path" -> "D:/DataSet/DataSet/SparkDataSet/locations.csv",
      "inferSchema" -> "true",
      "nullValue"-> "NA",
      "sep" -> ",",
      "compression" -> "snappy", // bzip2, gzip, lz4, deflate, uncompressed
      "dateFormat" -> "dd/MM/yyyy",
      "allowSingleQuotes" -> "true"
    ))
    .load()
  //  convert the upper case column to lower case
  val cln_loc_df = loc_df.selectExpr("LOCATION_ID as LocID", "STREET_ADDRESS as Street", "POSTAL_CODE as Zip", "City as City",
    "STATE_PROVINCE as State", "COUNTRY_ID as CountryID")

  //  read the employees datafile from the location
  val emp_df = spark.read
    .format("csv")
    .options(Map(
      "header" -> "true",
      "mode" -> "PERMISSIVE",
      "path" -> "D:/DataSet/DataSet/SparkDataSet/employees.csv",
      "inferSchema" -> "true",
      "nullValue"-> "NA",
      "sep" -> ",",
      "compression" -> "snappy", // bzip2, gzip, lz4, deflate, uncompressed
      "dateFormat" -> "dd/MM/yyyy",
      "allowSingleQuotes" -> "true"
    ))
    .load()
  //  convert the upper case column to lower case
  val cln_emp_df = emp_df.selectExpr("EMPLOYEE_ID as EmpID", "FIRST_NAME as F_Name", "LAST_NAME as L_Name", "Email as Email",
    "PHONE_NUMBER as Contact", "HIRE_DATE as Hire_DT", "JOB_ID as Job_ID", "Salary as Salary", "COMMISSION_PCT as Commission",
    "MANAGER_ID as Manager_ID", "DEPARTMENT_ID as DeptID")

  def main(args: Array[String]): Unit = {

    val dept_tab = cln_dept_df.createOrReplaceTempView("Departments")
    val loc_tab = cln_loc_df.createOrReplaceTempView("Locations")
    val emp_tab = cln_emp_df.createOrReplaceTempView("Employees")

    val SQL_qry = spark.sql(
      """
        |select e.EmpID, e.f_name||' '||e.l_name as Name,
        |l.locID, d.Dept_ID, d.Dept_Name, l.city as City, l.state as State
        |from
        |Locations l join departments d
        |on l.locID = d.Loc_ID join employees e
        |on e.DeptID = d.Dept_ID
        |""".stripMargin)
    SQL_qry.show(false)
    println(s"Records Effected: ${SQL_qry.count()}")

    spark.stop()
  }
}