package com.zumsoft.casestudy.publisher

import java.time.LocalDateTime

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.zumsoft.casestudy.models.{DeviceReading, ReplyTo}
import io.circe.generic.auto._
import io.circe.parser
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.wordspec.AnyWordSpecLike

class PublisherMainSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  "device" must {
    "reply to ReplyTo message with DeviceReading" in {
      val deviceId = java.util.UUID.randomUUID
      val unit = "unit"
      val version = 1
      val device = spawn(Device(deviceId, unit, version))
      val replyProbe = createTestProbe[DeviceReading]()
      device ! ReplyTo(replyProbe.ref)

      val message = replyProbe.expectMessageType[DeviceReading]
      message.deviceId shouldBe deviceId
      message.currentValue shouldBe 50F +- 50F
      message.unit shouldBe unit
      message.timeStamp.isBefore(LocalDateTime.now()) shouldBe true
      message.version shouldBe version
    }
  }

  "publisher" must {
    "receive a DeviceReading and publish DeviceData to Kafka" in {
      val mockProducer = new MockProducer[String, String](true, new StringSerializer(), new StringSerializer())
      val topic = "test-topic"
      val publisher = spawn(Publisher(mockProducer, topic))
      val deviceId = java.util.UUID.randomUUID
      val unit = "unit"
      val time = LocalDateTime.now()
      val version = 1
      val deviceReading = DeviceReading(deviceId, 0.1F, unit, time, version)
      publisher ! deviceReading

      // TODO avoid using thread sleep while waiting for message to be processed
      Thread.sleep(500)

      mockProducer.history.size shouldBe 1
      val parsedDeviceReading = parser.decode[DeviceReading](mockProducer.history.get(0).value()) match {
        case Right(parsedDeviceReading) => parsedDeviceReading
      }
      parsedDeviceReading shouldBe deviceReading
    }
  }

  "publisherMain" must {
    "receive any message and start child actors" in {
      val mockProducer = new MockProducer[String, String](true, new StringSerializer(), new StringSerializer())
      val topic = "test-topic"
      val publisherMain = spawn(PublisherMain(mockProducer, topic))
      publisherMain ! "start"
      // TODO write more comprehensive test
    }
  }

}
