package com.zumsoft.casestudy.subscriber

import java.time.Duration

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import com.zumsoft.casestudy.publisher.Publisher.DeviceReading
import com.zumsoft.casestudy.publisher.PublisherMain.ReplyTo
import io.circe.parser
import org.apache.kafka.clients.consumer.Consumer

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
          // send device reading to aggregator actor
          message.replyTo ! parsedDeviceReading
          // TODO send device data to DB

        case Left(ex) => s"There was an error parsing: $ex"
      }
    }
    Behaviors.same
  }
}

object SubscriberMain {

  def apply(kafkaConsumer: Consumer[String, String], topic: String): Behavior[String] =
    Behaviors.setup { context =>
      context.log.info("Bootstrapping SubscriberMain actors")
      val subscriber = context.spawn(Subscriber(kafkaConsumer, topic), "subscriber")
      Behaviors.receiveMessage { _ =>
        // Provides the consumer actor ref to the aggregator actor
        // TODO implement aggregator actor
        //subscriber ! ReplyTo(aggregator)
        Behaviors.same
      }
    }
}