package com.zumsoft.casestudy

import akka.actor.typed.ActorSystem
import com.zumsoft.casestudy.publish.PublisherMain
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.duration.Duration

object BootstrapActors extends App {
  val deviceMain: ActorSystem[String] = ActorSystem(PublisherMain(), "DeviceMain")
  // Scheduled message every 3 seconds to the guardian actor
  deviceMain.scheduler.scheduleAtFixedRate(Duration.Zero, 3.seconds)(() => deviceMain ! "start")
}
