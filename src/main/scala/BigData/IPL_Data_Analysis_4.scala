package BigData

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}

object IPL_Data_Analysis_4 {
  println("Indian Premier League Analysis 4")

  //  define spark session
  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Indian Premier League Analysis 4")
    .getOrCreate()

  //  define the schema for IPL DF
  val ipl_schema = StructType(List(
    StructField("id", IntegerType),
    StructField("city", StringType),
    StructField("schedule", DateType),
    StructField("player_of_match", StringType),
    StructField("venue", StringType),
    StructField("neutral_venue", StringType),
    StructField("team1", StringType),
    StructField("team2", StringType),
    StructField("toss_winner", StringType),
    StructField("toss_decision", StringType),
    StructField("winner", StringType),
    StructField("result", StringType),
    StructField("result_margin", StringType),
    StructField("eliminator", StringType),
    StructField("method", StringType),
    StructField("umpire1", StringType),
    StructField("umpire2", StringType),
  ))

  //  read the IPL DF
  spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
  val ipl_df = spark.read
    .format("csv")
    .option("header", "true")
    .schema(ipl_schema)
    .option("dateFormat", "yyyy-MM-dd")
    .option("mode", "failFast") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .option("path","D:/DataSet/DataSet/SparkDataSet/IndianPremierLeague.csv")
    .load()

  def compute() = {
    //  select the required columns
    import spark.implicits._
    val ipl_sel = ipl_df.select(col("city"), col("schedule"), expr("player_of_match as MOM"), 'venue,
      'team1, 'team2, column("winner"), column("result"), $"result_margin", 'eliminator)

    // case expression using withColumn
    //  ipl_sel.withColumn("bat_bowl_df", expr("case when result = 'runs' then 'batting first'" +
    //    "when result = 'wickets' then 'bowling first'" +
    //    "else 'NoPlay' end")).show()

    // case expression using withColumn
    val bat_bowl_df = ipl_sel.select(col("*"),expr("case when result = 'runs' then 'batted first'" +
      "when result = 'wickets' then 'fielded first'" +
      "else 'NoPlay' end").alias("bat_bowl"))

    //  rename the existing column
    val ipl_data = bat_bowl_df.withColumnRenamed("bat_bowl", "batting_fielding")

    //  filter the DF using like operator
    //  val filtered_ipl_df = ipl_data.filter("winner like 'Mumbai%' " ).show()

    //  filter the DF using like, and, or operator
    val filtered_ipl_df = ipl_data.where("winner like 'Kolkata%' and result != 'runs'")
      .filter("city != 'Kolkata' and eliminator = 'N'")
      .distinct()

      filtered_ipl_df.show(false)

    println(s"Records Effected ${filtered_ipl_df.count()}")
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
