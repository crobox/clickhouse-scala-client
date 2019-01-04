package com.crobox.clickhouse

import org.joda.time.{DateTime, Interval}

package object time {
  type IntervalStart = DateTime

  implicit class FixedDurationExtension(multiDuration: MultiDuration) {
    def millis(): Long = multiDuration.asPeriod.toStandardDuration.getMillis
  }

  implicit class IntervalExtras(obj: DateTime) {

    def to(endInterval: DateTime) = new Interval(obj, endInterval)
  }
}
