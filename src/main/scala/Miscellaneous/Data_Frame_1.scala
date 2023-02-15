package Miscellaneous

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.expressions.Window

object Data_Frame_1 extends App{
  println("Churn Modeling 1")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Churn Modeling 1")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig).getOrCreate()

  val schema = StructType( Array(
    StructField("Company", StringType,true),
    StructField("Model", StringType,true),
    StructField("OS", StringType, true),
    StructField("Price", StringType, true),
    StructField("Date", StringType, true)
  ))

  val data = Seq(
    Row("Samsung", "Galaxy s8", "Android" ,65000, "25-01-2017"),
    Row("Apple", "IPhone 10 MAX", "iOS", 75000, "12-09-2020"),
    Row("Apple", "IPhone X", "iOS", 125000, "12-09-2020"),
    Row("Redmi", "Redmi 9", "Android", 10900,"12-09-2020"),
    Row("Samsung", "Galaxy s21", "Android", 74000, "20-12-2020")
  )

//  val df = spark.createDataFrame(data, schema)

  spark.stop()

}

