package Bank

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.count

object People_JSON extends App {
  println("People's JSON File")
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("People's JSON File")
    .getOrCreate()

  val people_df = spark.read.json("D:/DataSet/DataSet/SparkDataSet/people.json")

    people_df.show(10, false)
  spark.stop()
}
