package summingbird.proto

import java.io.PrintWriter
import java.util.Properties
import java.util.concurrent.Executors

import org.slf4j.LoggerFactory

import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector}

object RunIngestion extends App {
  Ingestion.run
}

object Ingestion {
  private val logger = LoggerFactory.getLogger(this.getClass)

  import ViewCount._

  val RESTART_CONSUMER_PERIOD_MS = 60000L

  val props = new Properties()
  props.put("zookeeper.connect", KafkaZkConnectionString)
  props.put("group.id", "summingbird-prototype-ingestion")
  props.put("zookeeper.session.timeout.ms", "400");
  props.put("zookeeper.sync.time.ms", "200");
  props.put("auto.commit.interval.ms", "1000");

  val config = new ConsumerConfig(props);

  val consumer = Consumer.create(config)

  val streams = consumer.createMessageStreams(Map(KafkaTopic -> 1)).get(KafkaTopic).getOrElse {
    throw new IllegalArgumentException("Unable to connect to topic: " + KafkaTopic)
  }

  val executor = Executors.newFixedThreadPool(1)

  var ingested = 0L

  def newCurrentFile = dataFileForBatch(batcher.currentBatch)

  def run(): Unit = run(() => ())

  def run(callback: () => Unit): Unit = {

    var currentFile: String = newCurrentFile
    var buffer = Seq.empty[ProductViewed]

    for (stream <- streams) {
      val r = new Runnable() {
        override final def run: Unit = {
          try {
            stream.iterator.foreach { messageAndMetadata =>
              logger.debug("Consumed %s from kafka".format(messageAndMetadata))

              val pageView = parseView(messageAndMetadata.message)

              if (newCurrentFile != currentFile) {
                val writer = new PrintWriter(currentFile)

                for (pdpView <- buffer) {
                  writer.println(serializeView(pdpView))
                }

                writer.close()

                callback()

                buffer = Seq.empty[ProductViewed]
                currentFile = newCurrentFile
              }

              buffer +:= pageView
              ingested += 1
            }
          } catch {
            // There was a problem talking to Kafka.  We need to handle this situation.  Let's wait a bit and try again.
            case ex: Exception => {
              logger.error("Unable to talk to Kafka.  Attempting to restart the consumer in " + RESTART_CONSUMER_PERIOD_MS + " ms.", ex)
              Thread.sleep(RESTART_CONSUMER_PERIOD_MS)
              executor.submit(this)
            }
          }
        }
      }

      executor.submit(r)
    }
  }
}
