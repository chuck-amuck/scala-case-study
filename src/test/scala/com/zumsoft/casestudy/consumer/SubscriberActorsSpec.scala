package com.zumsoft.casestudy.consumer

import java.time.LocalDateTime
import java.util

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.zumsoft.casestudy.publisher.Publisher.DeviceReading
import com.zumsoft.casestudy.publisher.PublisherMain.ReplyTo
import com.zumsoft.casestudy.subscriber.{Subscriber, SubscriberMain}
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.kafka.clients.consumer.{ConsumerRecord, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.TopicPartition
import org.scalatest.wordspec.AnyWordSpecLike

class SubscriberActorsSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  "subscriber" must {
    "consume records from Kafka and return DeviceInfo" in {

      val topic = "test-topic"
      val topicPartition = new TopicPartition(topic, 1)
      val beginningOffsets = new util.HashMap[TopicPartition, java.lang.Long]
      val deviceReading = DeviceReading(java.util.UUID.randomUUID.toString, 0.1F, "unit", LocalDateTime.now().toString, 1)
      beginningOffsets.put(topicPartition, 0L)
      val record = new ConsumerRecord[String, String](topic, 1, 0L, null, deviceReading.asJson.noSpaces)

      val mockConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)
      mockConsumer.subscribe(util.Arrays.asList(topic))
      mockConsumer.rebalance(util.Arrays.asList(topicPartition))
      mockConsumer.updateBeginningOffsets(beginningOffsets)
      mockConsumer.addRecord(record)

      val replyProbe = createTestProbe[DeviceReading]
      val subscriber = spawn(Subscriber(mockConsumer, topic))

      subscriber ! ReplyTo(replyProbe.ref)

      replyProbe.expectMessageType[DeviceReading] shouldBe deviceReading
    }
  }

  "subscriberMain" must {
    "receive any message and start child actors" in {
      val mockConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)
      val topic = "test-topic"
      val subscriberMain = spawn(SubscriberMain(mockConsumer, topic))
      subscriberMain ! "start"
      // TODO write more comprehensive test
    }
  }
}
