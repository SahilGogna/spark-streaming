package part3lowlevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
  * @author sahilgogna on 2020-05-15
  */
object DStreamWindowTransformation {

  val spark = SparkSession.builder()
    .appName("DSWindowTras")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readLines() : DStream[String] = ssc.socketTextStream("localhost",9090)

  /*
  window = keep all the values emitted between now and X time back
  window interval updated with every batch
  window interval must be a multiple of the batch interval
   */
  def linesByWindow: DStream[String] = readLines().window(Seconds(10))

  /*
  first arg = window duration
  second arg = sliding duration
   */
  def linesBySlidingWindow() : DStream[String] = readLines().window(Seconds(10), Seconds(5))

  // count the number of elements over a window
  def countLinesByWindow(): DStream[Long] = readLines().countByWindow(Minutes(60), Seconds(30))

  // aggregate data in a different way over a window
  def sumAllTextByWindow() = readLines().map(_.length).window(Seconds(10), Seconds(5)).reduce(_+_)

  // identical
  def sumAllTextByWindiwAlt() = readLines().map(_.length).reduceByWindow(_+_, Seconds(10), Seconds(5))

  def main(args: Array[String]): Unit = {

  }
}
