package com.crobox.clickhouse

import java.time.{Duration => JavaDuration, ZonedDateTime}

package object time {
  type IntervalStart = ZonedDateTime

  implicit class FixedDurationExtension(multiDuration: MultiDuration) {
    def millis(): Long = multiDuration.millis
  }

  implicit class IntervalExtras(obj: ZonedDateTime) {
    def to(endInterval: ZonedDateTime): TimeInterval = TimeInterval(obj, endInterval)
  }
}

package time {

  /** Replaces joda's `Interval`, which `MultiInterval` used to extend. */
  case class TimeInterval(start: ZonedDateTime, end: ZonedDateTime) {
    def duration: JavaDuration = JavaDuration.between(start, end)
  }
}
