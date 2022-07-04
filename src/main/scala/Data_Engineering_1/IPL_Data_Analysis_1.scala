package Data_Engineering_1


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object IPL_Data_Analysis_1 {
  println(s"IPL Data Analysis 1")

  //  define spark session
  val sparkAppConf = new SparkConf()
  sparkAppConf.set("spark.app.name", "IPL Data Analysis 1")
  sparkAppConf.set("spark.master", "local[3]")

  val spark = SparkSession.builder()
    .config(sparkAppConf)
    .getOrCreate()

  //  define the schema for IPL DF
  val ipl_schema = StructType(List(
    StructField("id", IntegerType),
    StructField("city", StringType),
    StructField("schedule", StringType),
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
    .option("dateFormat", "dd-MM-yyyy")
    .option("mode", "failFast") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .option("path","D:/DataSet/DataSet/SparkDataSet/IndianPremierLeague.csv")
    .load()

  def compute() = {
    //  repartition the DF
    val ipl_part = ipl_df.repartition(5)

    //  limit the DF using where transformation
    val ipl_sel = ipl_part.where("city = 'Kolkata'")
      .selectExpr("city", "schedule", "player_of_match as MOM", "venue", "team1", "team2", "winner")

    // display the schema and the show the records
    ipl_sel.printSchema()
    ipl_sel.show(false)
    println(s"Records Effected: ${ipl_sel.count()}")
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}