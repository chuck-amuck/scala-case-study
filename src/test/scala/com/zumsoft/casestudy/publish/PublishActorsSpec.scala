package com.zumsoft.casestudy.publish

import java.time.LocalDateTime

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.zumsoft.casestudy.publish.Publisher.DeviceReading
import com.zumsoft.casestudy.publish.PublisherMain.ReplyTo
import org.scalatest.wordspec.AnyWordSpecLike

class PublishActorsSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  "A device" must {
    "reply to ReplyTo with DeviceReading" in {
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

  "A publisher" must {
    "receive a DeviceReading and publish DeviceData to kafka" in {

      val publisher = spawn(Publisher())
      val deviceId = java.util.UUID.randomUUID.toString
      val unit = "unit"
      val version = 1
      publisher ! DeviceReading(deviceId, 0.1F, unit, LocalDateTime.now().toString, version)
      // TODO publish to kafka
    }
  }

  "publisherMain" must {
    "receive any message and start child actors" in {

      val publisherMain = spawn(PublisherMain())
      publisherMain ! "start"
      // TODO write more comprehensive test
    }
  }

}
