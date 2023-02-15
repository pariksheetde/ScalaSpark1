package BigData

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType


object Cars_Schema_1 extends App {
  println("German Cars using DataFrame 1")

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("German Cars using DataFrame 1")
    .getOrCreate()

  //  define data using seq and row
  val data = Seq(
    Row(100, "Audi", "Audi Q4"),
    Row(110, "BMW", "BMW X4"),
    Row(120, "Mercedes", "Maybach Mercedes")
  )

  val schema = StructType(Array(
    StructField("ID",IntegerType,true),
    StructField("Model",StringType,true),
    StructField("Brand", StringType)
  ))
  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(data),schema)
  df.printSchema()
  df.show()
  spark.stop()
}
