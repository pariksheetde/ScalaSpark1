package BigData

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType, FloatType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}

object Video_Games_Analysis_1 {
  println("Video Games Analysis 1")

  //  define spark session
  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Video Games Analysis 1")
    .getOrCreate()

  //  define schema for video games DF
  val vg_schema = StructType(Array(
    StructField("Rank", IntegerType),
    StructField("Name", StringType),
    StructField("Platform", StringType),
    StructField("Year", IntegerType),
    StructField("Genre", StringType),
    StructField("Publisher", StringType),
    StructField("NA_Sales", FloatType),
    StructField("EU_Sales", FloatType),
    StructField("JP_Sales", FloatType),
    StructField("Other_Sales", FloatType),
    StructField("Global_Sales", FloatType)
  ))

  //  read the video games DF
  spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
  val games_df = spark.read
    .format("csv")
    .option("header", "true")
    .schema(vg_schema)
//  .option("dateFormat", "yyyy-MM-dd")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive, failFast
    .option("sep", ",")
    .option("nullValue", "")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .option("path","D:/DataSet/DataSet/SparkDataSet/vgsales.csv")
    .load()

  def compute() = {
    val ms_agg = games_df.select("Publisher", "NA_Sales")
      .groupBy("Publisher")
      .agg(
        mean("NA_Sales").as("mean_NA_Sales"),
        stddev("NA_Sales").as("std_NA_Sales"))
      .filter("Publisher in ('Microsoft Game Studios', 'Nintendo')")
      .orderBy(col("std_NA_Sales").desc)
      .show(truncate = false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }

}
