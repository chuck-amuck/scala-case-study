package com.zumsoft.casestudy.models

import akka.actor.typed.ActorRef

final case class ReplyTo(replyTo: Set[ActorRef[DeviceReading]])

object ReplyTo {
  def apply(ref: ActorRef[DeviceReading]): ReplyTo = ReplyTo(Set(ref))
}


