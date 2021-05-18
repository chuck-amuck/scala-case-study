package com.zumsoft.casestudy

import java.util.Properties

import akka.actor.typed.ActorSystem
import com.typesafe.config.ConfigFactory
import com.zumsoft.casestudy.config.{CaseStudyConfig, KafkaConsumerConfig, KafkaProducerConfig}
import com.zumsoft.casestudy.publisher.PublisherMain
import com.zumsoft.casestudy.subscriber.SubscriberMain
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, _}

object BootstrapActors extends App {
  private val config = ConfigFactory.load()
  private val caseStudyConfig = CaseStudyConfig.load(config)
  private val kafkaProducerConfig = caseStudyConfig.kafkaProducerConfig
  private val kafkaConsumerConfig = caseStudyConfig.kafkaConsumerConfig


  private val publisherMain: ActorSystem[String] = ActorSystem(PublisherMain(initializeProducer(kafkaProducerConfig), kafkaProducerConfig.topic), "PublisherMain")
  private val subscriberMain: ActorSystem[String] = ActorSystem(SubscriberMain(initializeConsumer(kafkaConsumerConfig), kafkaConsumerConfig.topic), "SubscriberMain")
  // Scheduled message every 3 seconds to the PublisherMain guardian actor
  publisherMain.scheduler.scheduleAtFixedRate(Duration.Zero, 3.seconds)(() => publisherMain ! "start")
  // Scheduled message every 3 seconds to the SubscriberMain guardian actor
  subscriberMain.scheduler.scheduleAtFixedRate(Duration.Zero, 3.seconds)(() => subscriberMain ! "start")

  private def initializeProducer(config: KafkaProducerConfig): Producer[String, String] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, s"${config.host}:${config.port}")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.RETRIES_CONFIG, "5")
    new KafkaProducer[String, String](props)
  }

  private def initializeConsumer(config: KafkaConsumerConfig): Consumer[String, String] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, s"${config.host}:${config.port}")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId)
    new KafkaConsumer[String, String](props)
  }
}
