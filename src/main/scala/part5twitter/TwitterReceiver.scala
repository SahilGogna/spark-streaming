package part5twitter

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import twitter4j.{StallWarning, Status, StatusDeletionNotice, StatusListener, TwitterStream, TwitterStreamFactory}

import scala.concurrent.Promise
import java.io.{OutputStream, PrintStream}

class TwitterReceiver extends Receiver[Status](StorageLevel.MEMORY_ONLY){

  import scala.concurrent.ExecutionContext.Implicits.global

  val twitterStreamPromise = Promise[TwitterStream]
  val twitterStreamFuture = twitterStreamPromise.future

  private def SimpleStatusListener = new StatusListener {

    override def onStatus(status: Status): Unit = store(status)

    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = ()

    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = ()

    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = ()

    override def onStallWarning(warning: StallWarning): Unit = ()

    override def onException(ex: Exception): Unit = ex.printStackTrace()
  }

  private def redirectSystemError() = System.setErr(new PrintStream( new OutputStream{
    override def write(b: Array[Byte]): Unit = ()
    override def write(b: Array[Byte], off: Int, len: Int): Unit = ()
    override def write(b: Int): Unit = ()
  }))

  override def onStart(): Unit = {
    redirectSystemError()
    val twitterStream: TwitterStream = new TwitterStreamFactory("src/main/resources")
      .getInstance()
      .addListener(SimpleStatusListener)
      .sample("en")

    twitterStreamPromise.success(twitterStream)
  }

  override def onStop(): Unit = twitterStreamFuture.foreach{ twitterStream =>
    twitterStream.cleanUp()
    twitterStream.shutdown()
  }
}
