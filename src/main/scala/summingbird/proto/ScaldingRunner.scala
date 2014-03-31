package summingbird.proto

import com.twitter.summingbird._
import com.twitter.summingbird.batch.{BatchID, Timestamp}
import com.twitter.summingbird.batch.state.HDFSState
import com.twitter.summingbird.scalding._
import com.twitter.summingbird.scalding.store.{InitialBatchedStore,VersionedStore}
import com.twitter.storehaus.memcache.MemcacheStore
import com.twitter.scalding._
import com.twitter.util.Await
import org.apache.hadoop.conf.Configuration

import org.slf4j.LoggerFactory


/**
  * The following object contains code to execute the Summingbird
  * ViewCount job defined in ExampleJob.scala on a Scalding
  * cluster.
  */
object ScaldingRunner {
  @transient private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * These imports bring the requisite serialization injections, the
    * time extractor and the batcher into implicit scope. This is
    * required for the dependency injection pattern used by the
    * Summingbird Scalding platform.
    */
  import Serialization._, ViewCount._

  /**
    * This takes a DateRange and determines the set of log files
    * which contain events during that interval.
    */
  val sourceFactory: DateRange => Mappable[String] = { dr =>

    logger.debug("DateRange: %s".format(dr))

    val start = batcher.batchOf(dr.start.value)
    val end = batcher.batchOf(dr.end.value)

    val batches: Seq[BatchID] = Stream.iterate(start)(_ + 1).takeWhile(_ <= end)

    logger.debug("batches: %s".format(batches))

    val files = batches.map { dataFileForBatch(_) }

    logger.debug("files: %s".format(files))

    MultipleTextLineFiles(files: _*)
  }

  /**
    * Creates a Source of ProductViewed events from the log files
    * for processing
    */

  val pipe: PipeFactory[ProductViewed] = Scalding.mappedPipeFactory(sourceFactory) (parseView(_))
  val source = Producer.source[Scalding, ProductViewed](pipe)

  /**
    * Store for results of batches.
    *
    * This is an HDFS SequenceFile which is efficient to write to,
    * but not appropriate for serving (no random key access)
    */

  val versionedStore = VersionedStore[Long, Long](JobDir + "/store")
  // won't worry about any historical data for this demo
  val store = new InitialBatchedStore(batcher.currentBatch, versionedStore)

  /**
    * For serving, we need to load the results of the batch into
    * a ReadableStore.  Normally this would be a persistent store,
    * but just using Memcache here for simplicity.
    */
  val servingStore =
    MemcacheStore.typed[Long, (BatchID, Long)](MemcacheStore.defaultClient("memcached", "localhost:11211"), "scaldingLookCount")

  val mode = Hdfs(false, new Configuration())

  /**
    * Actually runs the job
    */

  var first = true
  def runOnce() = {
    val job = Scalding("SummingbirdPrototypeScalding")

    val batch = batcher.currentBatch

    /**
      * Keeps the current state of which batches have been processed already.
      * The first time, we need to specify to begin with the last completed
      * batch, but after that HDFSState should keep track for us.
      */
    val startTime = if (first) Some(batcher.earliestTimeOf(batch - 1L)) else None
    val waitingState = HDFSState(JobDir + "/waitstate", startTime = startTime)

    /**
      * Actually run the job for this batch
      */

    job.run(waitingState, mode, job.plan(viewCount[Scalding](source, store)))

    SequenceFileReader[Long,Long](sequenceFile(batch)) { (k, bAndV) =>
      logger.debug("storing " + k + " : " + bAndV)
      servingStore.put((k,Some(bAndV)))
    }

  }

  def sequenceFile(batch: BatchID) =
    JobDir + "/store/" + batcher.earliestTimeOf(batch).milliSinceEpoch


  def main(args: Array[String]) {
    import java.util.concurrent._

    val ex = Executors.newSingleThreadScheduledExecutor

    ex.scheduleAtFixedRate(
      new Runnable { def run = runOnce },
      0, batcher.durationMillis, TimeUnit.MILLISECONDS
    )

  }

  def lookupHDFS() {
    logger.info("\nRESULTS: \n")

    var total = 0L

    SequenceFileReader[Long,Long](sequenceFile(batcher.currentBatch)) { (k, bAndV) =>
      logger.info(k + " : " + bAndV._2)
      total += bAndV._2
    }

    logger.info("total : " + total)
  }

  def lookup(lookId: Long): Option[(BatchID,Long)] =
    Await.result {
      servingStore.get(lookId)
    }

}


object SequenceFileReader {
  import com.twitter.bijection.{ Codec, Injection }
  import com.twitter.summingbird.batch.BatchID

  import java.util.Arrays

  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.io.{ BytesWritable, SequenceFile }
  import org.apache.hadoop.fs.{ FileSystem, Path }
  import Serialization._

  def deserializeKey[V](bytes: Array[Byte])(implicit inj: Injection[V, Array[Byte]], c: Codec[V]) = inj.invert(bytes)
  def deserializeValue[V](bytes: Array[Byte])(implicit inj: Injection[(BatchID, V), Array[Byte]], c: Codec[V]) = inj.invert(bytes)

  def apply[K,V](basefile: String)(fn: (K,(BatchID,V)) => Unit)
    (implicit kInj: Injection[K, Array[Byte]], vInj: Injection[(BatchID, V), Array[Byte]], c: Codec[V])
  = {

    /*
     * There can be one or more part-NNNNN files.
     * I'm not really sure how to predict this,
     * and the API doesn't seem to have a simple
     * way to just read them all
     */

    Stream.from(0)
      .map(i => "%s/part-%05d".format(basefile, i))
      .takeWhile(filename => new java.io.File(filename).exists)
      .foreach { filename =>
        val path = new Path(filename)
        val config = new Configuration()
        val reader = new SequenceFile.Reader(FileSystem.getLocal(config), path, config)
        val key = new BytesWritable()
        val value = new BytesWritable()
        while (reader.next(key, value)) {
          val keyBytes = Arrays.copyOfRange(key.getBytes, 0, key.getLength)
          val valueBytes = Arrays.copyOfRange(value.getBytes, 0, value.getLength)
          val decodedKey = deserializeKey[K](keyBytes).get
          val decodedBatchAndValue = deserializeValue[V](valueBytes).get
          fn(decodedKey, decodedBatchAndValue)
        }
        reader.close()
      }
  }

}
