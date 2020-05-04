package part2structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author sahilgogna on 2020-05-04
  */
object SteamingJoins {

   val spark = SparkSession.builder()
     .appName("Streaming Joins")
     .master("local[2]")
     .getOrCreate()

  val guitarPlayersDF = spark.read
    .option("inferSchema",true)
    .json("src/main/resources/data/guitarPlayers")

  val guitarsDF = spark.read
    .option("inferSchema",true)
    .json("src/main/resources/data/guitars")

  val bandsDF = spark.read
    .option("inferSchema",true)
    .json("src/main/resources/data/bands")

  // joining static Data frames
  val joinCondition = guitarPlayersDF.col("band") === bandsDF.col("id")
  val guitaristBands = guitarPlayersDF.join(bandsDF,joinCondition,"inner")
  val bandSchema = bandsDF.schema


  def joinStreamWithStatic() = {

    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9090)
      .load()
      .select(from_json(col("value"),bandSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name",
        "band.hometown as hometown", "band.year as year")

    // join happens per batch
    val streamedBandsGuitaristDF = streamedBandsDF
      .join(guitarPlayersDF, guitarPlayersDF.col("band") === streamedBandsDF.col("id")
      , "inner")

    /**
      restricted joins:
      - stream join with static : Right joins of all type not permitted
      - static joining with stream : Left joins of all types not permitted
      */

    streamedBandsGuitaristDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  // since Spark 2.3 we have stream vs Stream joins
  // it is similar to the upper example but you have to listen to 2 different ports for 2 streams
  // you want to join.
  // - inner joins are supported
  // left/right outer joins are supperted, but must have watermarks
  // full outer joins are not supported


  def main(args: Array[String]): Unit = {
    joinStreamWithStatic()
  }

}
