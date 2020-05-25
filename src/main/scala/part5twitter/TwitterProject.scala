package part5twitter

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

object TwitterProject {

  val spark = SparkSession.builder()
    .appName("Twitter Project")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readTwitter(): Unit = {
    val twitterStream : DStream[Status] = ssc.receiverStream(new TwitterReceiver)

    val tweets : DStream[String] = twitterStream.map{ status =>
      val userName = status.getUser.getName
      val followers = status.getUser.getFollowersCount
      val text = status.getText

      s"User $userName ($followers followers) says: $text"
    }
    tweets.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def getAverageTweetLength() : DStream[Double] = {
    val tweets = ssc.receiverStream(new TwitterReceiver)
    tweets
      .map(status => status.getText())
      .map(text => text.length)
      .map(len => (len,1))
      .reduceByWindow((tuple1,tuple2) => (tuple1._1 + tuple2._1, tuple1._2+tuple2._2) ,Seconds(5),Seconds(5))
      .map{ bigTuple =>
        val tweetLength = bigTuple._1
        val tweetCount = bigTuple._2
        tweetLength*1.0 / tweetCount
      }
  }

  def getPopularHashTags(): DStream[(String,Int)] = {
    val tweets = ssc.receiverStream(new TwitterReceiver)
    ssc.checkpoint("checkpoints")
    tweets
      .flatMap(status => status.getText.split(" "))
      .filter( word => word.startsWith("#"))
      .map(hashCount => (hashCount, 1))
      .reduceByKeyAndWindow((x,y) => x+y, (x,y)=> x-y, Seconds(60), Seconds(10))
      .transform( rdd => rdd.sortBy( tuple => - tuple._2))
  }

  def readTwitterWithSentiments(): Unit = {
    val twitterStream: DStream[Status] = ssc.receiverStream(new TwitterReceiver)
    val tweets: DStream[String] = twitterStream.map { status =>
      val username = status.getUser.getName
      val followers = status.getUser.getFollowersCount
      val text = status.getText
      val sentiment = SentimentAnalysis.detectSentiment(text) // a single "marker"

      s"User $username ($followers followers) says $sentiment: $text"
    }
    tweets.print()
    ssc.start()
    ssc.awaitTermination()
  }


    def main(args: Array[String]): Unit = {
      readTwitterWithSentiments()
  }

}
