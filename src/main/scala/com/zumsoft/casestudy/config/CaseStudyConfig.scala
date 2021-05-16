package com.zumsoft.casestudy.config

import com.typesafe.config.Config
import pureconfig._
import pureconfig.generic.auto._

case class KafkaConfig(host: String, port: Int, topic: String)

case class CaseStudyConfig(kafkaConfig: KafkaConfig)

object CaseStudyConfig {
  def load(config: Config): CaseStudyConfig = {
    ConfigSource.fromConfig(config).loadOrThrow[CaseStudyConfig]
  }
}
