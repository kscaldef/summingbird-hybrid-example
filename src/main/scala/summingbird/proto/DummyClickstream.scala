package summingbird.proto

import kafka.producer.{Producer, ProducerConfig, KeyedMessage}
import kafka.serializer.StringEncoder
import java.util.Properties

import org.slf4j.LoggerFactory

object RunDummyClickstream extends App {
  DummyClickstream.run()
}

object DummyClickstream {
  private val logger = LoggerFactory.getLogger(this.getClass)

  val props = new Properties()
  props.put("metadata.broker.list", "localhost:9092")
  props.setProperty("key.serializer.class", classOf[StringEncoder].getName)

  lazy val producer = new Producer[String, Array[Byte]](new ProducerConfig(props))

  var produced = 0L

  def run() =
    while (true) {
      val pdpView = randomView()

      logger.debug(s"sending $pdpView")
      producer.send(new KeyedMessage(KafkaTopic, pdpView.hashCode.toString, serializeView(pdpView).getBytes))
      produced += 1

      Thread.sleep(1000)
    }

}
