package com.zumsoft.casestudy.publish

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}

object Helper {

  import java.time.LocalDateTime

  def randomFloat(): Float = scala.util.Random.between(0F, 100F)

  def now(): String = LocalDateTime.now.toString

  def UUID(): String = java.util.UUID.randomUUID.toString
}

object Publisher {

  final case class DeviceReading(deviceId: String, currentValue: Float, unit: String, timeStamp: String, version: Int)

  def apply(): Behavior[DeviceReading] =
    Behaviors.receive { (context, message) =>
      context.log.info("Publisher received data: {}", message)
      // TODO Publish data to Kafka
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

  def apply(): Behavior[String] =
    Behaviors.setup { context =>
      context.log.info("Bootstrapping actors")
      val device1 = context.spawn(Device(Helper.UUID(), "Fahrenheit", 1), "thermostat")
      val device2 = context.spawn(Device(Helper.UUID(), "Fahrenheit", 1), "water_heater")
      val device3 = context.spawn(Device(Helper.UUID(), "Fahrenheit", 3), "hvac")
      val device4 = context.spawn(Device(Helper.UUID(), "Watts", 2), "elevator")
      val device5 = context.spawn(Device(Helper.UUID(), "RPM", 5), "fan")
      val publisher = context.spawn(Publisher(), "publisher")

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

