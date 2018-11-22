package com.crobox.clickhouse.time

import org.joda.time.Period

sealed trait Duration {
  val unit: TimeUnit
}

/**
 * Parses a duration expressed in string to the corresponding value.
 * It accepts input in the format `{value}{label}` or `{label}`.
 * Examples: 1h, 2hours, 4day, day, month, 1M
  **/
object Duration {
  private val DurationRegex = "(\\d+)?(\\D+)".r

  def parse(expression: String): Duration =
    expression match {
      case DurationRegex(null, unit) =>
        TimeUnit.lookup(unit).forValue(1)
      case DurationRegex(value, unit) =>
        TimeUnit.lookup(unit).forValue(value.toInt)
      case _ =>
        throw new IllegalArgumentException(s"Cannot parse a duration from $expression.")
    }
}

case class MultiDuration(value: Int, override val unit: MultiTimeUnit) extends Duration {
  val asPeriod: Period = unit.asPeriod.multipliedBy(value)
}

object MultiDuration {
  def apply(unit: MultiTimeUnit): MultiDuration = MultiDuration(1, unit)
}

case class SimpleDuration(override val unit: SimpleTimeUnit) extends Duration
