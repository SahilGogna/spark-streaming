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

  def computeWordOccurancesByWindow() = {
    // for reduce by key and window we need a checkpoint directory set
    ssc.checkpoint("checkpoints")

    readLines()
      .flatMap(_.split(" "))
      .map( word => (word,1))
      .reduceByKeyAndWindow(
        (a,b) => a+b, // reduce function
        (a,b) => a-b, // "inverse function" -> to reduce the count of dropping words with window slide
        Seconds(60), // window duration
        Seconds(10) // sliding duration
      )
  }

  /**
    * Exercise
    * Word longer than 10 chars => $2
    * Every other word => $0
    *
    * Input text into the terminal => money made over the past 30 seconds, updated every 10 seconds.
    * - use window
    * - use count by window
    * - use reduceByWindow
    * - use reduceByKeyAndWindow
    */

    val moneyPerExpensiveWord = 2
    def countIncome1() = {
      readLines()
        .flatMap(_.split(" "))
        .filter(_.length() >= 10)
        .map(_ => moneyPerExpensiveWord)
        .reduce(_+_)
        .window(Seconds(30), Seconds(10))
        .reduce(_+_)
    }

  def countIncome2() = {
    readLines()
      .flatMap(_.split(" "))
      .filter(_.length() >= 10)
      .countByWindow(Seconds(30), Seconds(10))
      .map(_*moneyPerExpensiveWord)
  }

  def countIncome3() = {
    readLines()
      .flatMap(_.split(" "))
      .filter(_.length() >= 10)
      .map(_ => moneyPerExpensiveWord)
      .reduceByWindow(_+_,Seconds(30), Seconds(10))
  }

  def countIncome4() = {
    ssc.checkpoint("checkpoints")
    readLines()
      .flatMap(_.split(" "))
      .filter(_.length() >= 10)
      .map{ word =>
        if (word.length() >=10) ("expensive",2)
        else ("cheap",0)
      }
      .reduceByKeyAndWindow(_+_,_-_,Seconds(30),Seconds(10))
  }

  def main(args: Array[String]): Unit = {
    computeWordOccurancesByWindow().print()
    ssc.start()
    ssc.awaitTermination()
  }
}
