package Data_Engineering_2

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, max, mean, min, round, sum}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.count

object FIFA_Stats_Aggregation_3 extends App {
  println("FIFA World Cup Aggregation Analysis 3")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "FIFA World Cup Aggregation Analysis 3")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .getOrCreate()

  //  read the FIFA datafile from the location
  val fifa_df = spark.read
    .format("csv")
    .options(Map(
      "header" -> "true",
      "mode" -> "PERMISSIVE",
      "path" -> "D:/DataSet/DataSet/SparkDataSet/FIFA_Stats.csv",
      "inferSchema" -> "true",
      "nullValue"-> "NA",
      "sep" -> ",",
      "compression" -> "snappy", // bzip2, gzip, lz4, deflate, uncompressed
      "dateFormat" -> "dd/MM/yyyy",
      "allowSingleQuotes" -> "true"
    ))
    .load()

  import spark.implicits._
  val sel_fifa = fifa_df.selectExpr("Year", "Datetime", "Stage as RoundRobin", "City", "Home_Team_Name", "Home_Team_Goals",
    "Away_Team_Goals", "Away_Team_Name",
    "Half_Time_Home_Goals as 1st_Half_Home_Goals", "Half_Time_Away_Goals as 1st_Half_Away_Goals",
    "Home_Team_Goals - Half_Time_Home_Goals as 2nd_Half_Home_Goals",
    "Away_Team_Goals - Half_Time_Away_Goals as 2nd_Half_Away_Goals")
    .withColumn("Goals", col("Home_Team_Goals") + col( "Away_Team_Goals"))
    .sort(col("Year").asc,col("Home_Team_Name").asc)

  sel_fifa.printSchema()
  sel_fifa.show()
  println(s"Records Effected: ${sel_fifa.count()}")

  //  select the columns required for aggregation
  val sel_agg_cols = sel_fifa.select("Year", "Home_Team_Name", "Goals")
  sel_agg_cols.show()


  val sql_df = sel_agg_cols.createOrReplaceTempView("Goals_Aggregation")
  val qry_df = spark.sql(
    """select * from (select
      |Year, Home_Team_Name,
      |sum(goals) over (partition by Home_Team_Name, Year order by Goals desc) as Sum_Goals,
      |dense_rank() over (partition by Home_Team_Name, Year order by Goals asc) as DRank
      |from
      |Goals_Aggregation
      |order by Year asc, Sum_Goals desc) as Temp where DRank = 1""".stripMargin)

  qry_df.show()
  println(s"Records Effected: ${qry_df.count()}")

  val top_goals = qry_df.createOrReplaceTempView("Top_Goals_Scorer")
  val qry_df_agg = spark.sql(
    """
      |select Year, Home_Team_Name, Sum_Goals,
      |dense_rank(Sum_Goals) over (partition by Year order by Year) as DRank,
      |rank(Sum_Goals) over (partition by Year order by Year) as Rank
      |from Top_Goals_Scorer
      |order by Year, Sum_Goals desc
      |""".stripMargin)

  qry_df_agg.show()
  spark.stop()
}
