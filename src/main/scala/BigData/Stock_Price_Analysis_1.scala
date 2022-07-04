package BigData

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType

object Stock_Price_Analysis_1 {
  println("Stock Price Analysis")

  //  define spark session
  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Stock Price Analysis")
    .getOrCreate()

  //  define schema for stock
  val stock_schema = StructType(Array(
    StructField("StkDate", DateType),
    StructField("Open", DoubleType),
    StructField("High", DoubleType),
    StructField("Low", DoubleType),
    StructField("Close", DoubleType),
    StructField("AdjClose", DoubleType),
    StructField("Volume", DoubleType)
  ))
  spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
  //  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  //  https://stackoverflow.com/questions/62602720/string-to-date-migration-from-spark-2-0-to-3-0-gives-fail-to-recognize-eee-mmm

  //  read the csv file into DF
  val stock_df = spark.read
    .format("csv")
    .option("header", "true")
    .schema(stock_schema)
    .option("dateFormat", "YYYY-dd-MM")
    .option("mode", "failFast") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .option("path","D:/DataSet/DataSet/SparkDataSet/StockPrice.csv")
    .load()

  def compute() = {
    val clean_stk = stock_df.selectExpr("StkDate", "Open", "High", "Low", "Close", "AdjClose", "Volume")
      .show(false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
