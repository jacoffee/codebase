package org.apache.spark.streaming

import java.util.Properties
import java.util.concurrent.locks.ReentrantLock
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.spark.streaming.util.RecurringTimer
import org.apache.spark.util.{EventLoop, SystemClock}

/*
  + Eventloop learn from Spark
  + behavior related to KafkaMessageProducer encapsulated as case class/object Event
*/

sealed trait KafkaMessageEvent
case class GenerateMessages(time: Time) extends KafkaMessageEvent

abstract class KafkaMessageProducer {

  private val lock = new ReentrantLock()
  private val condition = lock.newCondition()

  var eventLoop: EventLoop[KafkaMessageEvent] = null
  var producer: KafkaProducer[String, String] = null

  // execution interval
  protected val durationInMillis = Seconds(30).milliseconds
  val timer = new RecurringTimer(
    new SystemClock, durationInMillis,
    longTime => eventLoop.post(GenerateMessages(new Time(longTime))), "MessageGenerator"
  )

  def start(): Unit = {
    eventLoop = new EventLoop[KafkaMessageEvent]("MessageGenerator") {
      override protected def onReceive(event: KafkaMessageEvent): Unit = processEvent(event)
      override protected def onError(e: Throwable): Unit = throw e
    }
    eventLoop.start()
    producer = getKafkaProducer()

    val startTime = new Time(timer.getStartTime())
    timer.start(startTime.milliseconds)
  }

  def getKafkaProducer() = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }


  // called by EventLoop's onReceive to handle the logic
  def processEvent(kafkaMessageEvent: KafkaMessageEvent) = {
    kafkaMessageEvent match {
      case GenerateMessages(time) => send(producer, time)
    }
  }

  def send(producer: KafkaProducer[String, String], time: Time): Unit

  def stop(): Unit = {
    timer.stop(true)
    eventLoop.stop()
  }

  // used to block the program
  def waitTillEnd(): Unit = {
    lock.lock
    try {
      condition.await
    } finally {
      lock.unlock
    }
  }

}
