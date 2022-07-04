package BigData

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}

object IMDB_Movies_Analysis_2 {
  println("IMDb Movies Analysis 2")

  //  define spark session
  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("IMDb Movies Analysis 2")
    .getOrCreate()

  //  define the schema for IPL DF
  val movies_schema = StructType(List(
    StructField("imdb_title_id", StringType),
    StructField("title", StringType),
    StructField("original_title", StringType),
    StructField("year", IntegerType),
    StructField("date_published", StringType),
    StructField("genre", StringType),
    StructField("duration", StringType),
    StructField("country", StringType),
    StructField("language", StringType),
    StructField("director", StringType),
    StructField("writer", StringType),
    StructField("production_company", StringType),
    StructField("actors", StringType),
    StructField("description", StringType),
    StructField("avg_vote", StringType),
    StructField("votes", StringType),
    StructField("budget", StringType),
    StructField("usa_gross_income", StringType),
    StructField("worlwide_gross_income", StringType),
    StructField("metascore", StringType),
    StructField("reviews_from_users", StringType),
    StructField("reviews_from_critics", StringType)
  ))

  //  read the IPL DF
  spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
  val movies_df = spark.read
    .format("csv")
    .option("header", "true")
    .schema(movies_schema)
//  .option("dateFormat", "yyyy-MM-dd")
    .option("mode", "permissive") // dropMalFormed, permissive, failFast
    .option("sep", ",")
    .option("nullValue", "")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .option("path","D:/DataSet/DataSet/SparkDataSet/IMDb_Movies.csv")
    .load()

  def compute() = {
    //  select the required columns, filter out unnecessary details
    import spark.implicits._
    val movies_sel = movies_df.select(col("title"), expr("year"), 'date_published,
      'genre, 'duration, column("country"), column("language"), $"director", expr("reviews_from_users as users_rating"),
      expr("reviews_from_critics as critics_rating"))
      .filter("country = 'USA'")
      .where("year > 2000")

      movies_sel.show(truncate = false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
