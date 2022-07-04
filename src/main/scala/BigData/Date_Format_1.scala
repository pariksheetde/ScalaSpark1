package BigData

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, dayofmonth, month, to_date, to_timestamp, year}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object Date_Format_1 {
  println("Date Format using DataFrame 1")

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("Date Format using DataFrame 1")
    .getOrCreate()

//  define the schema
  val def_schema = StructType(List(
    StructField("ID", IntegerType, true),
    StructField("EventDate", StringType, true)
  ))

  def compute() = {
    import spark.implicits._
    val columns = Seq("ID","EventDate")
    val data = Seq((100, "01/01/2020"), (110, "02/02/2020"), (120, "03/03/2020"))
    val df = spark.createDataFrame(data).toDF(columns:_*)

    //  before conversion from string type to date type
    df.printSchema()
    df.show()

    import spark.implicits.StringToColumn
    val sel_df = df.select($"ID", $"EventDate").show()
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
