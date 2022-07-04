package Data_Engineering_2

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}


object Departments_Analysis_1 {
  println("Departments Analysis")

  //  create a spark session
  val sparkAppConf = new SparkConf()
  sparkAppConf.set("spark.app.name", "Departments Analysis")
  sparkAppConf.set("spark.master", "local[3]")

  val spark = SparkSession.builder()
    .config(sparkAppConf)
    .getOrCreate()

  //  define schema for Dept
  val dept_sch = StructType(List(
    StructField("dept_id", IntegerType, false),
    StructField("dept_name", StringType),
    StructField("loc_id", IntegerType)
  ))

  //  read a dept file
  val dept_df = spark.read
    .format("csv")
    .schema(dept_sch)
    .option("header", "true")
    .load("D:/DataSet/DataSet/SparkDataSet/departments.csv")

  val sel_dept = dept_df.selectExpr("dept_id as DeptID", "dept_name as Dept_Nm", "loc_id as LocID")

  def main(args: Array[String]): Unit = {
    sel_dept.printSchema()
    sel_dept.show(false)
    spark.stop()
  }
}