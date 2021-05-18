package com.zumsoft.casestudy.consumer

import java.time.LocalDateTime
import java.util
import java.util.UUID

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.zumsoft.casestudy.models.{DeviceReading, ReplyTo}
import com.zumsoft.casestudy.subscriber.{Aggregator, DBWriter, Subscriber, SubscriberMain}
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.kafka.clients.consumer.{ConsumerRecord, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.TopicPartition
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpecLike
import scalikejdbc.{ConnectionPool, DB, _}

class SubscriberMainSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with BeforeAndAfterEach {

  ConnectionPool.singleton("jdbc:h2:~/test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE", "", "")

  override def beforeEach() {
    DB localTx { implicit session =>
      sql"""CREATE TABLE DEVICE_READINGS (
             ID        UUID  NOT NULL,
             VALUE     float(6) NOT NULL,
             UNIT      text NOT NULL,
             TIMESTAMP TIMESTAMP NOT NULL,
             VERSION   SMALLINT NOT NULL)"""
        .update().apply()
    }
  }

  override def afterEach() {
    DB localTx { implicit session =>
      sql"""DROP ALL OBJECTS""".update().apply()
    }
  }

  "subscriber" must {
    "consume records from Kafka and return DeviceInfo" in {

      val topic = "test-topic"
      val topicPartition = new TopicPartition(topic, 1)
      val beginningOffsets = new util.HashMap[TopicPartition, java.lang.Long]
      val deviceReading = DeviceReading(java.util.UUID.randomUUID, 0.1F, "unit", LocalDateTime.now(), 1)
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
  // TODO avoid using in memory DB, use scalikejdbc testkit
  "dbwriter" must {
    "receive DeviceInfo message and insert the fields into postgres" in {
      val aggregator = spawn(DBWriter())
      val deviceReading = DeviceReading(java.util.UUID.randomUUID, 0.1F, "unit", LocalDateTime.now(), 1)
      aggregator ! deviceReading

      // TODO avoid using thread sleep while waiting for message to be processed
      Thread.sleep(500)
      // TODO use QueryDSL for select
      val result = DB localTx { implicit session =>
        sql"""select * from device_readings""".map(rs =>
          DeviceReading(
            UUID.fromString(rs.string("id")),
            rs.float("value"),
            rs.string("unit"),
            rs.localDateTime("timestamp"),
            rs.int("version")))
          .single().apply()
      }
      result.isDefined shouldBe true
      result.get shouldBe deviceReading
    }
  }

  "aggregator" must {
    "aggregate DeviceInfo values" in {
      val aggregator = spawn(Aggregator())
      aggregator ! DeviceReading(java.util.UUID.randomUUID, 0.1F, "unit", LocalDateTime.now(), 1)
      // TODO write more comprehensive test
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
