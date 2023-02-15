package Miscellaneous

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Customer_JSON {
  println("Customer JSON")

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Customer JSON")
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()

  val cust_schema = StructType(
    List(
      StructField("customer_id", IntegerType, true),
      StructField("firstname", StringType, true),
      StructField("lastname", StringType, true),
      StructField("salutation", StringType, true),
      StructField("gender", StringType, true),
      StructField("demographics",
                  StructType(Array(
                    StructField("income_range", ArrayType(IntegerType, true), true),
                        StructField("buy_potential", StringType, true),
                        StructField("purchase_estimate", IntegerType, true),
                        StructField("vehicle_count", IntegerType, true),
                        StructField("credit_rating", StringType, true),
                        StructField("education_status", StringType, true)
                           )
                ),true),
      StructField("birthdate", DateType, true),
      StructField("birth_country", StringType, true),
      StructField("address_id", IntegerType, true),
      StructField("email_address", StringType, true),
      StructField("is_preffered_customer", StringType, true)
    )
  )

  val cust_df = spark.read.format("json")
    .schema(cust_schema)
    .load("D:/DataSet/DataSet/customer.json")

  def main(args: Array[String]): Unit = {

//  select
//    val cust_sel_df = cust_df.select(col("customer_id"),
//                                     col("firstname"), col("lastname"),
//                                     concat(col("firstname"), lit(" "), col("lastname")).alias("fullname"),
//                                     col("demographics.buy_potential"),
//                                     col("demographics.credit_rating"), col("demographics.purchase_estimate"),
//                                     col("demographics.vehicle_count"), col("demographics.education_status"),
//                                     col("demographics.income_range"),
//                                     col("birthdate"),
//                                     dayofmonth(col("birthdate")).alias("birth_dayz"),
//                                     month(col("birthdate")).alias("birth_month"),
//                                     year(col("birthdate")).alias("birth_year")
//                                    )

    //  selectExpr
    val cust_sel_df = cust_df.selectExpr(
      "customer_id",
      "firstname", "lastname",
      "demographics.education_status", "demographics.income_range",
      "demographics.income_range[0] as lower_bound",
      "demographics.income_range[1] as upper_bound",
      "demographics.buy_potential", "demographics.credit_rating",
      "demographics.purchase_estimate", "demographics.vehicle_count",
      "birthdate")
      .withColumn("birth_dayz", dayofmonth(col("birthdate")))
      .withColumn("birth_month", month(col("birthdate")))
      .withColumn("birth_year", year(col("birthdate")))
      .withColumn("fullname", concat(col("firstname"), lit(" "), col("lastname")))
    cust_sel_df.printSchema()
    cust_sel_df.show(10, false)

    spark.stop()
  }
}