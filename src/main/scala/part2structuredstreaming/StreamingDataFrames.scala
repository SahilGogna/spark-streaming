package part2structuredstreaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import common._

import scala.concurrent.duration._

/**
  * @author sahilgogna on 2020-05-03
  */
object StreamingDataFrames {

  val spark : SparkSession= SparkSession.builder()
    .appName("Spark Streams")
    .master("local[2]")
    .getOrCreate()

  def readFromSocket():Unit = {
    // reading a DF
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9090)
      .load()

    // adding transformations
    val shortLines: DataFrame = lines.select(col("value"))
      .where(length(col("value")) <= 5)

    // how to check static vs streaming DF
    println(shortLines.isStreaming)

    // consuming a DF
    val query: StreamingQuery = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    // .start is asynchronous, we need to wait for the stream to finish
    query.awaitTermination()
  }

  def readFromFiles():Unit = {
    // we have to always specify a schema for streaming data
    // schema is already defined in the options folder

    val stocksDF= spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

    // now this might seems almost same as static data frames but the stock folder contains
    // many files and if we add new files, Spark will keep an eye and add the data to the dataframe

  }

  def demoTriggers():Unit = {
    // every 2 seconds run the query, df is checked for new data,
    // transformations are executed on new batch, if data is empty it will check for the
    // next batch

    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9090)
      .load()

    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(
        Trigger.ProcessingTime(2.seconds)
//        Trigger.Continuous(2.seconds) // experimental, every 2 sec creates a batch with whatever you have
//          Trigger.Once()
      )
      .start()
      .awaitTermination()
  }



  def main(args: Array[String]): Unit = {
    demoTriggers()
  }

}
