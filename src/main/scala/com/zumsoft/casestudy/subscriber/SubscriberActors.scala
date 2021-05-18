package com.zumsoft.casestudy.subscriber

import java.time.Duration

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import com.zumsoft.casestudy.publisher.Publisher.DeviceReading
import com.zumsoft.casestudy.publisher.PublisherMain.ReplyTo
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
        case Right(parsedDeviceReading) =>
          println(parsedDeviceReading)
          message.replyTo ! parsedDeviceReading
        case Left(ex) => s"There was an error parsing: $ex"
      }
    }
    Behaviors.same
  }
}

object Aggregator {
  // TODO only pass deviceId and currentValue, no need to pass whole DeviceReading object
  def apply(): Behavior[DeviceReading] = aggregate()

  private def aggregate(aggregatedValues: Map[String, List[Float]] = HashMap[String, List[Float]]()): Behavior[DeviceReading] =
    Behaviors.receive { (_, message) =>
      def update(key: String, value: Float): Map[String, List[Float]] =
      // If key exists for deviceId, appends currentValue to the underlying list, otherwise creates a new key with the value
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
      Behaviors.receiveMessage { _ =>
        // Provides the subscriber actor ref to the aggregator actor
        subscriber ! ReplyTo(aggregator)
        Behaviors.same
      }
    }
}
