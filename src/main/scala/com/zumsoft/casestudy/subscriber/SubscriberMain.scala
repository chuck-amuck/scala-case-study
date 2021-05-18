package com.zumsoft.casestudy.subscriber

import java.time.Duration
import java.util.UUID

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import com.zumsoft.casestudy.models.{DeviceReading, ReplyTo}
import io.circe.parser
import org.apache.kafka.clients.consumer.Consumer

import scala.collection.immutable.HashMap

object Subscriber {
  def apply(kafkaConsumer: Consumer[String, String], topic: String): Behavior[ReplyTo] =
    Behaviors.setup(context => new Subscriber(context, kafkaConsumer, topic))
}

// TODO use dependency injection to provide the kafka consumer and topic
class Subscriber(context: ActorContext[ReplyTo], kafkaConsumer: Consumer[String, String], topic: String) extends AbstractBehavior[ReplyTo](context) {

  import io.circe.generic.auto._

  kafkaConsumer.subscribe(java.util.Arrays.asList(topic))

  override def onMessage(message: ReplyTo): Behavior[ReplyTo] = {
    kafkaConsumer.poll(Duration.ofSeconds(3)).records(topic).forEach { record =>
      parser.decode[DeviceReading](record.value()) match {
        case Right(deviceReading) =>
          message.replyTo.foreach(ref => ref ! deviceReading)
        case Left(ex) => s"There was an error parsing: $ex"
      }
    }
    Behaviors.same
  }
}

object DBWriter {
  def apply(): Behavior[DeviceReading] =
    Behaviors.setup(context => new DBWriter(context))
}

class DBWriter(context: ActorContext[DeviceReading]) extends AbstractBehavior[DeviceReading](context) {

  import scalikejdbc._

  // TODO investigate the feasibility of batching
  // TODO use async library for DB connection
  // TODO use QueryDSL for insert
  override def onMessage(message: DeviceReading): Behavior[DeviceReading] = {
    DB localTx { implicit session =>
      sql"""insert into device_readings (id, value, unit, timestamp, version) values (${message.deviceId}, ${message.currentValue},
            ${message.unit}, ${message.timeStamp}, ${message.version})"""
        .update().apply()
    }
    Behaviors.same
  }
}

object Aggregator {
  // TODO only pass deviceId and currentValue, no need to pass whole DeviceReading object
  def apply(): Behavior[DeviceReading] = aggregate()

  // TODO introduce logic to configure maximum size of collection of values and add automatic eviction
  private def aggregate(aggregatedValues: Map[UUID, List[Float]] = HashMap[UUID, List[Float]]()): Behavior[DeviceReading] =
    Behaviors.receive { (_, message) =>
      // If key exists for deviceId, appends currentValue to the underlying list, otherwise creates a new key with the value converted to a list
      def update(key: UUID, value: Float): Map[UUID, List[Float]] =
        aggregatedValues + (key -> (aggregatedValues.getOrElse(key, List[Float]()) :+ value))

      aggregate(update(message.deviceId, message.currentValue))
    }
}

object SubscriberMain {
  def apply(kafkaConsumer: Consumer[String, String], topic: String): Behavior[String] =
    Behaviors.setup { context =>
      context.log.info("Bootstrapping SubscriberMain actors")
      val subscriber = context.spawn(Subscriber(kafkaConsumer, topic), "subscriber")
      val aggregator = context.spawn(Aggregator(), "aggregator")
      val dBWriter = context.spawn(DBWriter(), "dbwriter")
      Behaviors.receiveMessage { _ =>
        // Provides the aggregator and dbwriter actor refs to the subscriber actor
        subscriber ! ReplyTo(Set(aggregator, dBWriter))
        Behaviors.same
      }
    }
}
