package com.zumsoft.casestudy.config

import com.typesafe.config.Config
import pureconfig._
import pureconfig.generic.auto._

case class KafkaProducerConfig(host: String, port: Int, topic: String)
case class KafkaConsumerConfig(host: String, port: Int, topic: String, groupId: String)

case class CaseStudyConfig(kafkaProducerConfig: KafkaProducerConfig, kafkaConsumerConfig: KafkaConsumerConfig)

object CaseStudyConfig {
  def load(config: Config): CaseStudyConfig = {
    ConfigSource.fromConfig(config).loadOrThrow[CaseStudyConfig]
  }
}
