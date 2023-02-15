package Bank

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Churn_Model_Agg_10 {
  println("Aggregation on Churn Modeling 10")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "churn_model_agg_10")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig).getOrCreate()

//  read the data from the location
  val churn = spark.read
    .option("inferSchema", "true")
    .option("header", true)
    .csv("D:/DataSet/DataSet/SparkDataSet/ChurnModeling.csv")

  def compute() = {
    val churn_sel = churn.selectExpr("CustomerId as Cust_ID", "Geography as Country", "Gender as Sex",
      "Age", "Tenure","Balance", "HasCrCard as Credit_Card",
      "IsActiveMember as Active", "EstimatedSalary as Salary")
      .filter("Country in ('France','Spain')")
      .filter("Age >= 18")
      .filter("Active = 1")
      .where("Balance > 0")
      .sort(col("Cust_ID").asc)

    churn_sel.show(10, false)
    println(s"Records Effected: ${churn_sel.count()}")
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
