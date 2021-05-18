package com.zumsoft.casestudy.publisher

import java.time.LocalDateTime

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.zumsoft.casestudy.publisher.Publisher.DeviceReading
import com.zumsoft.casestudy.publisher.PublisherMain.ReplyTo
import io.circe.generic.auto._
import io.circe.parser
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.wordspec.AnyWordSpecLike

class PublisherActorsSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  "device" must {
    "reply to ReplyTo message with DeviceReading" in {
      val deviceId = java.util.UUID.randomUUID.toString
      val unit = "unit"
      val version = 1
      val device = spawn(Device(deviceId, unit, version))
      val replyProbe = createTestProbe[DeviceReading]()
      device ! ReplyTo(replyProbe.ref)

      val message = replyProbe.expectMessageType[DeviceReading]
      message.deviceId shouldBe deviceId
      message.currentValue shouldBe 50F +- 50F
      message.unit shouldBe unit
      LocalDateTime.parse(message.timeStamp).isBefore(LocalDateTime.now()) shouldBe true
      message.version shouldBe version
    }
  }

  "publisher" must {
    "receive a DeviceReading and publish DeviceData to Kafka" in {
      val mockProducer = new MockProducer[String, String](true, new StringSerializer(), new StringSerializer())
      val topic = "test-topic"
      val publisher = spawn(Publisher(mockProducer, topic))
      val deviceId = java.util.UUID.randomUUID.toString
      val unit = "unit"
      val time = LocalDateTime.now().toString
      val version = 1
      val deviceReading = DeviceReading(deviceId, 0.1F, unit, time, version)
      publisher ! deviceReading

      // TODO wait for call to be made to mock without sleep
      Thread.sleep(500)

      mockProducer.history.size shouldBe 1
      parser.decode[DeviceReading](mockProducer.history.get(0).value()) match {
        case Right(parsedDeviceReading) => parsedDeviceReading shouldBe deviceReading
        case Left(ex) => s"There was an error parsing: $ex"
      }
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
