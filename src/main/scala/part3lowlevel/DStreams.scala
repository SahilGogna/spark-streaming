package part3lowlevel

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

import common.Stock
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author sahilgogna on 2020-05-05
  */
object DStreams {

  val spark = SparkSession.builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  /**
    Spark Streaming context = entry point to the DStreams API
    - Needs 2 things
      => needs the spark context
      => a duration = batch interval
    */
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  /**
    - define input sources by creating DStreams
    - define transformations on DStreams
      => all the transformations are lazy i.e they are not executed until an action is performed
    - call an action on DStream
    - start ALL computations with ssc.start()
      => no more computations can be added
    - await termination, or stop the computation
      => you cannot restart the ssc
    */

  def readFromSocket():Unit = {
    val socketStream = ssc.socketTextStream("localhost",9090)

    // transformation are lazy
    val wordStream : DStream[String] = socketStream.flatMap( line => line.split(" "))

    // action
    // you can also write dstream as a textfile, .saveAsTextFile(path)
    wordStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def createNewFile() = {
    new Thread( () =>{
      Thread.sleep(5000)

      val path = "src/main/resources/data/stocks"
      val dir = new File(path) // directory where I will store the new file
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")
      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AMZN,Jan 1 2000,64.56
          |AMZN,Feb 1 2000,68.87
          |AMZN,Mar 1 2000,67
          |AMZN,Apr 1 2000,55.19
          |AMZN,May 1 2000,48.31
          |AMZN,Jun 1 2000,36.31
          |AMZN,Jul 1 2000,30.12
          |AMZN,Aug 1 2000,41.5
          |AMZN,Sep 1 2000,38.44
          |AMZN,Oct 1 2000,36.62
          |AMZN,Nov 1 2000,24.69
          |AMZN,Dec 1 2000,15.56
          |AMZN,Jan 1 2001,17.31
          |""".stripMargin.trim)

      writer.close()
    }).start()
  }

  def readFromFile() = {
    createNewFile()
    val textStream = ssc.textFileStream("src/main/resources/data/stocks")

    // transformations
    val dateFormat = new SimpleDateFormat("MMM d yyyy")
    val stocksStream = textStream.map{ line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dateFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble

      Stock(company,date,price)
    }

    // action
    stocksStream.print()

    // start the computations
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromFile()
  }


}
