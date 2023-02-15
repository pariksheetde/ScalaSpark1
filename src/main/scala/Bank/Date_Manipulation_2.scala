package Bank

import org.apache.spark.sql.DataFrame
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, spark_partition_id, to_date}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import javax.lang.model.`type`.IntersectionType
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.count


object Date_Manipulation_2 extends App {
  println("Date Manipulation using Seq")
  println("Date Manipulation 2")

  def dateconversion(df: DataFrame, fmt: String, fld: String): DataFrame = {
    df.withColumn(fld, to_date(col(fld),fmt))
}

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]").appName("Date Formatting")
    .getOrCreate()

  import spark.implicits._
  val columns = Seq("ID","EventDate")
  val data = Seq((100, "01/01/2020"), (110, "02/02/2020"), (120, "03/03/2020"))
  val df = spark.createDataFrame(data).toDF(columns:_*)

  df.printSchema()
  df.show()

  val func = dateconversion(df, "M/d/y", "EventDate")
  func.printSchema()
  func.show(10)

  spark.stop()
}
