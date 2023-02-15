package Bank

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, dayofmonth, expr, month, to_date, to_timestamp, year}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object Date_Manipulation_4 {
  println("Changing Data Format")
  println("Date Manipulation 4")

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("Changing Data Format")
    .getOrCreate()

  val columns = Seq("Date","Month", "Year")
  val data = Seq(("14", "09", "2000"),
    ("28", "02", "1999"))
  val df = spark.createDataFrame(data).toDF(columns:_*)

  def compute() = {
    val res_1 = df.select(col("Date"), col("Month"), col("Year"),
      expr("to_date(concat(Date, Month, Year),'ddMMyyyy') as Date_Of_Birth"))

    //  below code concat date, month, year
    //  val res_2 = df.selectExpr("Date", "Month", "Year",
    //  "to_date(concat(Date, Month, Year),'ddMMyyyy') as Date_of_Birth").show()

    import spark.implicits.StringToColumn
    val res_3 = df.select($"Date", $"Month", $"Year",
      to_date(concat($"Date", $"Month", $"Year"),"ddMMyyyy").as("Date_Of_Termination"))
      .show()
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
