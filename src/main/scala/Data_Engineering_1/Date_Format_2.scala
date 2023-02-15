package Data_Engineering_1


import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.monotonically_increasing_id

object Date_Format_2 extends App {
  println("Date Format 2")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Date Format 2 using DataFrame")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .getOrCreate()

  val data_schema = StructType(List(
    StructField("Name", StringType),
    StructField("Date", StringType),
    StructField("Month", StringType),
    StructField("Year", StringType)
  ))

  import spark.implicits._
  val columns = Seq("Name","Date", "Month", "Year")
  val data = Seq(("Monica", "12", "1", "2002"), ("Kate", "14", "9", "2010"), ("Peter", "31", "3", "05"), ("Pamela", "15", "6", "10"))
  val df = spark.createDataFrame(data).toDF(columns:_*)

  df.printSchema()
  df.show(false)

  val res_df = df.withColumn("id", monotonically_increasing_id())
    .withColumn("Date", col("Date").cast(IntegerType))
    .withColumn("Month", col("Month").cast(IntegerType))
    .withColumn("Year", col("Year").cast(IntegerType))
    .withColumn("Year", expr(
      """
        |case when Year < 21 then Year + 2000
        |when Year < 100 then Year + 1900
        |else Year
        |end
        |""".stripMargin))

  res_df.printSchema()
  res_df.show()
  spark.stop()
}