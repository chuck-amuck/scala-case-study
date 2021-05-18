package com.zumsoft.casestudy.publisher

import java.time.LocalDateTime
import java.util.UUID

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import com.zumsoft.casestudy.models.{DeviceReading, ReplyTo}
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}

object Helper {
  def randomFloat(): Float = scala.util.Random.between(0F, 100F)

  def UUID(): UUID = java.util.UUID.randomUUID
}

object Publisher {
  def apply(kafkaProducer: Producer[String, String], topic: String): Behavior[DeviceReading] =
    Behaviors.setup(context => new Publisher(context, kafkaProducer, topic))
}

// TODO use dependency injection to provide the kafka producer and topic
class Publisher(context: ActorContext[DeviceReading], kafkaProducer: Producer[String, String], topic: String) extends AbstractBehavior[DeviceReading](context) {

  import io.circe.generic.auto._
  import io.circe.syntax._

  override def onMessage(message: DeviceReading): Behavior[DeviceReading] = {
    val record = new ProducerRecord[String, String](topic, message.asJson.noSpaces)
    kafkaProducer.send(record)
    Behaviors.same
  }
}

object Device {
  def apply(id: UUID, unit: String, version: Int): Behavior[ReplyTo] = {
    Behaviors.receive { (_, message) =>
      // `val name` can be obtained from context.self.path.name
      message.replyTo.foreach { ref =>
        ref ! DeviceReading(id, Helper.randomFloat(), unit, LocalDateTime.now, version)
      }
      Behaviors.same
    }
  }
}

object PublisherMain {
  def apply(kafkaProducer: Producer[String, String], topic: String): Behavior[String] =
    Behaviors.setup { context =>
      context.log.info("Bootstrapping PublisherMain actors")
      val device1 = context.spawn(Device(Helper.UUID(), "Fahrenheit", 1), "thermostat")
      val device2 = context.spawn(Device(Helper.UUID(), "Fahrenheit", 1), "water_heater")
      val device3 = context.spawn(Device(Helper.UUID(), "Fahrenheit", 3), "hvac")
      val device4 = context.spawn(Device(Helper.UUID(), "Watts", 2), "elevator")
      val device5 = context.spawn(Device(Helper.UUID(), "RPM", 5), "fan")
      val publisher = context.spawn(Publisher(kafkaProducer, topic), "publisher")

      Behaviors.receiveMessage { _ =>
        // Provides the publisher actor ref to each device
        device1 ! ReplyTo(publisher)
        device2 ! ReplyTo(publisher)
        device3 ! ReplyTo(publisher)
        device4 ! ReplyTo(publisher)
        device5 ! ReplyTo(publisher)
        Behaviors.same
      }
    }
}

