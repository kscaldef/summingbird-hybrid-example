package com.twitter.tormenta.spout

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.{ Fields, Values }
import backtype.storm.utils.Time
import java.util.{ Map => JMap }
import java.util.Properties
import java.util.concurrent.{Executors, ExecutorService, LinkedBlockingQueue}

import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector}

/**
 *  Very simple KafkaSpout, don't use this for real!
 *  Just here to have something that works with Kafka 0.8
 *  and cross-builds
 */

object KafkaSpout {
  val RESTART_CONSUMER_PERIOD_MS = 60000L
}

class KafkaSpout[+T](zkConnectionString: String, topic: String, appID: String, nThreads: Int = 1)(fn: Array[Byte] => TraversableOnce[T])
    extends BaseRichSpout with Spout[T] {

  var collector: SpoutOutputCollector = null
  var consumer: ConsumerConnector = null

  var executor: ExecutorService = null

  lazy val limit = 1000
  lazy val queue = new LinkedBlockingQueue[Array[Byte]](limit)

  override def getSpout = this

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields(topic + appID)) // ???
  }

  override def open(conf: JMap[_, _], context: TopologyContext, coll: SpoutOutputCollector): Unit = {
    collector = coll

    val props = new Properties()
    props.put("zookeeper.connect", zkConnectionString)
    props.put("group.id", appID)
    props.put("zookeeper.session.timeout.ms", "400");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");

    val config = new ConsumerConfig(props);

    consumer = Consumer.create(config)

    val streams = consumer.createMessageStreams(Map(topic -> nThreads)).get(topic).getOrElse {
      throw new IllegalArgumentException("Unable to connect to topic: " + topic)
    }

    executor = Executors.newFixedThreadPool(nThreads)

    for (stream <- streams) {
      val r = new Runnable() {
        override final def run: Unit = {
          try {
            stream.iterator.foreach { messageAndMetadata =>
              //println("KafkaSpout consumed %s from kafka".format(messageAndMetadata))
              queue.offer(messageAndMetadata.message)
            }
          } catch {
            // There was a problem talking to Kafka.  We need to handle this situation.  Let's wait a bit and try again.
            case ex: Exception => {
              println("Unable to talk to Kafka.  Attempting to restart the consumer in " + KafkaSpout.RESTART_CONSUMER_PERIOD_MS + " ms.", ex)
              Thread.sleep(KafkaSpout.RESTART_CONSUMER_PERIOD_MS)
              executor.submit(this)
            }
          }
        }
      }

      executor.submit(r)
    }
  }

  /**
    * Override this to change the default spout behavior if poll
    * returns an empty list.
    */
  def onEmpty(): Unit = Time.sleep(1000)

  override def nextTuple {
    Option(queue.poll).map(fn) match {
      case None => onEmpty
      case Some(items) => items.foreach { item =>
        //println("emitting %s to storm".format(item))
        collector.emit(new Values(item.asInstanceOf[AnyRef]))
      }
    }
  }

  override def flatMap[U](newFn: T => TraversableOnce[U]): Spout[U] =
    new KafkaSpout(zkConnectionString, topic, appID, nThreads)(fn(_).flatMap(newFn))

  override def close {
    consumer.shutdown()
    executor.shutdown()
  }

}
