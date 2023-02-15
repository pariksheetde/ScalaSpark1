package Bank

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SQLContext, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, DateType, StringType, IntegerType}
import org.apache.spark.sql.types._
import java.util.Date

object IPL_Data_Analysis_2 {
  println("Indian Premier League DataSet API")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Indian Premier League")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig).getOrCreate()

  //spark.sql.shuffle.partitions configures the number of partitions that are used when shuffling data for joins or aggregations.
  spark.conf.set("spark.sql.shuffle.partitions",100)
  spark.conf.set("spark.default.parallelism",100)


  //  read the datafile from the location
  import spark.implicits._
  val ipl_df  = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
//  .schema(ipl_schema)
    .option("dateFormat", "M/D/Y")
    .option("mode", "FAILFAST")
    .csv("D:/DataSet/DataSet/SparkDataSet/IndianPremierLeague.csv")

  def compute() = {
    val ipl_sel_df = ipl_df.selectExpr("city", "schedule", "winner", "venue as stadium", "team1", "team2", "result", "neutral_venue")

    ipl_sel_df.show(20, truncate = false)
    ipl_sel_df.printSchema()
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
