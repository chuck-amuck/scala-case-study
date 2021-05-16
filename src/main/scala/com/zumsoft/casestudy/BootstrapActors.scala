package com.zumsoft.casestudy

import java.util.Properties

import akka.actor.typed.ActorSystem
import com.typesafe.config.ConfigFactory
import com.zumsoft.casestudy.config.{CaseStudyConfig, KafkaConfig}
import com.zumsoft.casestudy.publish.PublisherMain
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, _}

object BootstrapActors extends App {
  val config = ConfigFactory.load()

  private val caseStudyConfig = CaseStudyConfig.load(config)
  private val kafkaConfig = caseStudyConfig.kafkaConfig
  private val producer = initializeProducer(kafkaConfig)


  val deviceMain: ActorSystem[String] = ActorSystem(PublisherMain(producer, kafkaConfig.topic), "DeviceMain")
  // Scheduled message every 3 seconds to the guardian actor
  deviceMain.scheduler.scheduleAtFixedRate(Duration.Zero, 3.seconds)(() => deviceMain ! "start")


  private def initializeProducer(kafkaConfig: KafkaConfig): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, s"${kafkaConfig.host}:${kafkaConfig.port}")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.RETRIES_CONFIG, "5")

    new KafkaProducer[String, String](props)
  }

}
