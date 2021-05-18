package com.zumsoft.casestudy.models

import java.time.LocalDateTime
import java.util.UUID

final case class DeviceReading(deviceId: UUID, currentValue: Float, unit: String, timeStamp: LocalDateTime, version: Int)
