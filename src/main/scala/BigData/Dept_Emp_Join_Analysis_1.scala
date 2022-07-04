package BigData

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}

object Dept_Emp_Join_Analysis_1 {
  println("Department-Employee Analysis")

  //  define spark session
  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Department-Employee Analysis")
    .getOrCreate()

  //  define schema for department
  val dept_schema = StructType(List(
    StructField("DEPT_ID", IntegerType),
    StructField("DEPT_NAME", StringType),
    StructField("LOC_ID", IntegerType)
  ))

  //  define schema for employees
  val emp_schema = StructType(List(
    StructField("EMPLOYEE_ID", IntegerType),
    StructField("FIRST_NAME", StringType),
    StructField("LAST_NAME", StringType),
    StructField("EMAIL", StringType),
    StructField("PHONE_NUMBER", StringType),
    StructField("HIRE_DATE", StringType),
    StructField("JOB_ID", StringType),
    StructField("SALARY", IntegerType),
    StructField("COMMISSION_PCT", FloatType),
    StructField("MANAGER_ID", IntegerType),
    StructField("DEPARTMENT_ID", IntegerType)
  ))


  //  read the dept csv file
  spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
  val dept_df = spark.read
    .format("csv")
    .option("header", "true")
    .schema(dept_schema)
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive, failFast
    .option("sep", ",")
    .option("nullValue", "null")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .option("path","D:/DataSet/DataSet/SparkDataSet/departments.csv")
    .load()

//    dept_df.show(false)

  //  read the emp csv file
  spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
  val emp_df = spark.read
    .format("csv")
    .option("header", "true")
    .schema(emp_schema)
    .option("dateFormat", "dd-MM-yyyy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive, failFast
    .option("sep", ",")
    .option("nullValue", "")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .option("path","D:/DataSet/DataSet/SparkDataSet/employees.csv")
    .load()

//    emp_df.show(false)

  def compute() = {
    import spark.implicits._
    val dept_emp_join = dept_df.join(emp_df.withColumnRenamed("MANAGER_ID", "MAN_ID"),
      dept_df.col("DEPT_ID") === emp_df.col("DEPARTMENT_ID"), "inner")
      .selectExpr("EMPLOYEE_ID as emp_id", "FIRST_NAME as f_name", "LAST_NAME as l_name", "MAN_ID as manager_id",
        "DEPT_NAME as dept_name", "HIRE_DATE as joining_dt")

      dept_emp_join.show()
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }

}
