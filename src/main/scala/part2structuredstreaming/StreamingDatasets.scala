package part2structuredstreaming

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import common._

/**
  * @author sahilgogna on 2020-05-04
  */
object StreamingDatasets {

  val spark = SparkSession.builder()
    .appName("Streaming Data Sets")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  def getCars(): Dataset[Car] = {
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9090)
      .load() // df with single column "value"
      .select(from_json(col("value"), carsSchema).as("car")) // composite column (struct)
      .selectExpr("car.*") // df with multiple columns
      .as[Car]
  }

  def showCars(): Unit = {
    val carsDS: Dataset[Car] = getCars()

    // apply transformations
    val carNames: DataFrame = carsDS.select(col("name")) // this makes it a dataframe

    // we can also use normal methods
    // note they can be used to preserve type info
    // means in above example dataset was converted into dataframe, but now type will remain same

    val carNameNew: Dataset[String] = carsDS.map(_.Name)

    carNameNew.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  /**
    * Exercises:
    * 1) Count how many powerful cars we have in the DS (HP > 140)
    * 2) Average horsepower of the entire dataset
    * (use complete output mode)
    * 3) count the cars by origin
    */

  def showOutput(): Unit = {
    val carsDS: Dataset[Car] = getCars()

    val powerfulCars1 = carsDS.filter(_.Horsepower.getOrElse(0L) > 140).map(_.Name)
    val powerfulCars = carsDS.select(col("Name")).where(col("Horsepower").>(140))

    powerfulCars.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def ex2(): Unit = {
    val carsDS: Dataset[Car] = getCars()
    carsDS.select(avg(col("Horsepower"))).writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def ex3(): Unit = {
    val carsDS: Dataset[Car] = getCars()
    carsDS.groupBy(col("Origin")).count()
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    ex2()
  }

}
