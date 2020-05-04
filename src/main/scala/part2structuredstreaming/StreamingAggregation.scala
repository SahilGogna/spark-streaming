package part2structuredstreaming

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import part2structuredstreaming.StreamingDataFrames.spark
import org.apache.spark.sql.functions._

/**
  * @author sahilgogna on 2020-05-04
  */
object StreamingAggregation {

  val spark: SparkSession = SparkSession.builder()
    .appName("Spark Streams")
    .master("local[2]")
    .getOrCreate()

  def StreamingCount() = {
    // reading a DF
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9090)
      .load()

    val lineCount: DataFrame = lines.selectExpr("count(*) as linecount")

    // aggregations with distinct are not supported
    // otherwise Spark will need to keep track of Everything

    lineCount.writeStream
      .format("console")
      .outputMode("complete") // append and update not supported on aggregations without watermark
      .start()
      .awaitTermination()
  }

  def numericalAggregations(aggFunction: Column => Column) = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9090)
      .load()

    // aggregate here
    val numbers = lines.select(col("value").cast("integer").as("number"))
    //    val sumDF = numbers.select(sum(col("number")).as("Sum_so_far"))
    // we can make this function more powerful by passing an aggregation function
    val aggDF = numbers.select(aggFunction(col("number")).as("Sum_so_far"))

    aggDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def groupNames():Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port",9090)
      .load()

    // counting occurence of the "name" value
    val names = lines.select(col("value").as("name"))
      .groupBy(col("name")) // returns Relational grouped dataset
      .count()

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    groupNames()
  }

}
