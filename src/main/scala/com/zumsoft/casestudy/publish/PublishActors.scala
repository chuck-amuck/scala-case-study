package com.zumsoft.casestudy.publish

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.zumsoft.casestudy.publish.Publisher.DeviceReading
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}

object Helper {

  import java.time.LocalDateTime

  def randomFloat(): Float = scala.util.Random.between(0F, 100F)

  def now(): String = LocalDateTime.now.toString

  def UUID(): String = java.util.UUID.randomUUID.toString
}

object Publisher {

  final case class DeviceReading(deviceId: String, currentValue: Float, unit: String, timeStamp: String, version: Int)

  def apply(kafkaProducer: Producer[String, String], topic: String): Behavior[DeviceReading] =
    Behaviors.setup(context => new Publisher(context, kafkaProducer, topic))
}
// TODO use dependency injection to provide the kafka producer and topic
class Publisher(context: ActorContext[DeviceReading], kafkaProducer: Producer[String, String], topic: String) extends AbstractBehavior[DeviceReading](context) {

  import io.circe.generic.auto._
  import io.circe.syntax._

  override def onMessage(message: DeviceReading): Behavior[DeviceReading] = {
    context.log.info("Publisher received data: {}", message)
    val record = new ProducerRecord[String, String](topic, message.asJson.noSpaces)
    kafkaProducer.send(record)
    Behaviors.same
  }
}

object Device {

  def apply(id: String, unit: String, version: Int): Behavior[PublisherMain.ReplyTo] = {
    val deviceId = id
    val readingUnit = unit
    val deviceVersion = version
    // val createdAt = Helper.now()
    // `val name` can be obtained from context.self.path.name
    Behaviors.receive { (context, message) =>
      message.replyTo ! Publisher.DeviceReading(deviceId, Helper.randomFloat(), readingUnit, Helper.now(), deviceVersion)
      Behaviors.same
    }
  }
}

object PublisherMain {

  final case class ReplyTo(replyTo: ActorRef[Publisher.DeviceReading])

  def apply(kafkaProducer: Producer[String, String], topic: String): Behavior[String] =
    Behaviors.setup { context =>
      context.log.info("Bootstrapping actors")
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

