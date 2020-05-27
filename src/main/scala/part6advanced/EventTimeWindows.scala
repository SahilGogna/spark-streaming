package part6advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

object EventTimeWindows {

  val spark = SparkSession.builder()
    .appName("EventTimeWindows")
    .master("local[2]")
    .getOrCreate()

  val onlinePurchaseSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", StringType)
  ))

  def readPurchaseFromSocket() = spark.readStream
      .format("socket")
    .option("host","localhost")
    .option("port", 9090)
    .load()
    .select(from_json(col("value"), onlinePurchaseSchema).as("purchase"))
    .selectExpr("purchase.*")

  def aggregatePurchaseBySlidingWindow() = {
    val purchaseDF = readPurchaseFromSocket()

    val windowByDay = purchaseDF
      .groupBy(window(col("time"), "1 day", "1 hour").as("time"))// struct column: has fields {start, end}
      .agg(sum("quantity").as("totalQuantity"))

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def readFromFile() = spark.readStream
      .schema(onlinePurchaseSchema)
    .json("src/main/resources/data/purchases")

  // calculating best selling product of everyday and quantity sold
  def getBestSellingProduct() = {
    val purchaseDF = readFromFile()

    val bestSelling = purchaseDF
      .groupBy(col("item"),window(col("time"), "1 day").as("time")) // tumbling window: sliding duration == window duration
      .agg(sum("quantity").as("Total Quantity"))
        .select(
          col("time"),
          col("item"),
          col("Total Quantity")
        )
        .orderBy(col("time") , col("Total Quantity").desc)

    bestSelling.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    getBestSellingProduct()

  }

}
