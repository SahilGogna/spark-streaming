package part3lowlevel

import java.io.File
import java.sql.Date
import java.time.{LocalDate, Period}

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import common._
import org.apache.spark.streaming.dstream.DStream

object DStreamTransformations {

  val spark = SparkSession.builder()
    .appName("DStreamTransformations")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  import spark.implicits._
  def readPeople():DStream[Person] = ssc.socketTextStream("localhost", 9090).map{
    line =>
      val tokens = line.split(":")
      Person(
        tokens(0).toInt, // id
        tokens(1), // first name
        tokens(2), // middle name
        tokens(3), // last name
        tokens(4), // gender
        Date.valueOf(tokens(5)), // birth
        tokens(6), // ssn
        tokens(7).toInt // salary
      )
  }

  def peopleAges(): DStream[(String, Int)] = readPeople().map{ person =>
    val age = Period.between(person.birthDate.toLocalDate(), LocalDate.now()).getYears()
    (s"${person.firstName} ${person.lastName}",age)
  }

  def peopleSmallNames() :DStream[String] = readPeople().flatMap{person =>
    List(person.firstName, person.middleName)
  }

  def highIncomePeople() : DStream[Person] = readPeople().filter(_.salary > 75000)

  def countPeople(): DStream[Long] = readPeople().map(_.firstName).count() // the number of entries in every batch

  // count by value per batch
  def countNames() : DStream[(String, Long)] = readPeople().map(_.firstName).countByValue()

  // reduce by key
  // - works on DStream of tuple
  // - works per batch
  def countNamesReduce(): DStream[(String, Int)] =
    readPeople()
    .map(_.firstName)
    .map(name => (name,1))
    .reduceByKey(_+_)

  def saveToJson() = readPeople().foreachRDD{rdd =>
    val ds = spark.createDataset(rdd)
    val f = new File("sec/main/resources/data/people")
    val nFiles = f.listFiles().length
    val path = s"src/main/resources/data/people/people$nFiles.json"

    ds.write.json(path)

  }

  def main(args: Array[String]): Unit = {
    val stream = peopleAges()
    stream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
