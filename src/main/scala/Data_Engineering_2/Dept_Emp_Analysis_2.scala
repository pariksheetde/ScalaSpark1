package Data_Engineering_2

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, max, mean, min, round, sum}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.count

object Dept_Emp_Analysis_2 {
  println("Employees Analysis on Departments 2")
  println("Dept Emp Analysis 2")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Employees Analysis on Departments 2")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder
    .config(sparkAppConfig)
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
  val cln_dept_df = dept_df.selectExpr("Dept_ID as DeptID", "DEPT_NAME as Dept_Nm",
    "LOC_ID as LocID")

  //  read the employees datafile from the location
  val emp_df = spark.read
    .format("csv")
    .options(Map(
      "header" -> "true",
      "mode" -> "PERMISSIVE",
      "path" -> "D:/DataSet/DataSet/SparkDataSet/Employees.csv",
      "inferSchema" -> "true",
      "nullValue"-> "NA",
      "sep" -> ",",
      "compression" -> "snappy", // bzip2, gzip, lz4, deflate, uncompressed
      "dateFormat" -> "dd/MM/yyyy",
      "allowSingleQuotes" -> "true"
    ))
    .load()

  def compute() = {
    //  convert the upper case column to lower case
    val cln_emp_df = emp_df.selectExpr("EMPLOYEE_ID as EmpID", "FIRST_NAME as F_Name", "LAST_NAME as L_Name", "EMAIL as Email_ID",
      "PHONE_NUMBER as Contact", "HIRE_DATE as Joining_Date", "JOB_ID as Job_ID", "SALARY as Salary", "COMMISSION_PCT as Commission",
      "MANAGER_ID as Manager_ID", "DEPARTMENT_ID as DeptID")

    //  define the join condition between cln_dept_df and cln_emp_df
    val dept_emp_df_join_expr = cln_dept_df.col("DeptID") === cln_emp_df.col("DeptID")

    //  define the join
    val dept_emp_df = cln_dept_df.join(cln_emp_df, dept_emp_df_join_expr, "inner")

    //  drop the unwanted columns
    val sel_dept_emp_df = dept_emp_df.drop(cln_emp_df.col("DeptID"))
    sel_dept_emp_df.selectExpr("EmpID", "DeptID", "F_Name", "L_Name", "Email_ID", "Dept_Nm", "Contact")
      .show(false)

    println(s"Records Effected: ${sel_dept_emp_df.count()}")
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}