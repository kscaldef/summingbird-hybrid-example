package summingbird

package object proto {
  import ViewCount._
  import com.twitter.summingbird.batch.BatchID
  import java.text.SimpleDateFormat
  import java.util.{Date, TimeZone}

  val random = new scala.util.Random

  val JobDir = "/tmp/summingbird-proto/"
  val DataDir = JobDir + "data/"

  val KafkaZkConnectionString = "localhost:2181/kafka"
  val KafkaTopic = "summingbird.proto.productview"

  val MaxId = 10

  val DataFileDateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss")
  DataFileDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

  def dataFileForBatch(batch: BatchID) = {
    // timestamp is the end time of the batch
    DataDir + "productview_0_" + DataFileDateFormat.format(batcher.earliestTimeOf(batch.next).toDate)
  }

  def randomView(date: Date = new Date()) = {
    ProductViewed(
      random.nextLong.abs % MaxId,
      date,
      java.util.UUID.randomUUID.toString
    )
  }

  def parseView(bytes: Array[Byte]): ProductViewed = {
    parseView(new String(bytes))
  }

  def parseView(s: String): ProductViewed = {
    val bits = s.split("\t")
    ProductViewed(bits(0).toLong, new Date(bits(1).toLong), bits(2))
  }

  def serializeView(pdpView: ProductViewed): String = {
    "%s\t%s\t%s".format(pdpView.productId, pdpView.requestTime.getTime, pdpView.userGuid)
  }

}
