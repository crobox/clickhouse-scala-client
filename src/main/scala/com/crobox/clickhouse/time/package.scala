package com.crobox.clickhouse

import org.joda.time.{DateTime, Interval}

package object time {
  type IntervalStart = DateTime

  implicit class FixedDurationExtension(multiDuration: MultiDuration) {

    def millis(): Long =
      multiDuration match {
        case MultiDuration(value, fixedTimeUnit: FixedTimeUnit) =>
          fixedTimeUnit.standardMillis * value
        case _ => throw new IllegalArgumentException("Cannot calculate millis for non fixed duration.")
      }
  }

  implicit class IntervalExtras(obj: DateTime) {

    def to(endInterval: DateTime) = new Interval(obj, endInterval)
  }
}
