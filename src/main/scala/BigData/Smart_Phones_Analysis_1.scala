package BigData

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf

object Smart_Phones_Analysis_1 {
  println("Smart Phones Analysis using DataFrame 1")

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Smart Phones Analysis using DataFrame 1")
    .getOrCreate()

  val data = Seq(
    Row("Samsung", "Galaxy S8", "Android" ,65000, "15-01-2021"),
    Row("Apple", "IPhone 10 MAX", "iOS", 75000, "12-09-2020"),
    Row("Apple", "IPhone X", "iOS", 125000, "12-09-2020"),
    Row("Redmi", "Redmi 9", "Android", 10900,"12-09-2020")
  )

  val schema = StructType(List(
    StructField("Maker", StringType),
    StructField("Model", StringType),
    StructField("Operating_System", StringType),
    StructField("Price", IntegerType),
    StructField("Release_Date", StringType)
  ))

  def compute() = {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data), schema)

    df.printSchema()
    df.show()
    println(s"Number of rows in DF ${df.count()}")
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
