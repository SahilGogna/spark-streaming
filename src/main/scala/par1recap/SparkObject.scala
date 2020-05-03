package par1recap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author sahilgogna on 2020-05-03
  */
object SparkObject extends App {

  // entry point to the spark structured Api
  val spark = SparkSession.builder()
    .appName("Recap")
    .master("local")
    .getOrCreate()

  // read a df
  val cars = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  cars.show()
  cars.printSchema()

  import spark.implicits._
  // select

  val carsSelected = cars.select(
    col("name"),
    $"year",
    (col("weight_in_lbs")/2.2) as("weight_in_kgs"),
    expr("weight_in_lbs / 2.2").as("weight_in_kgs_2")
  )


}
