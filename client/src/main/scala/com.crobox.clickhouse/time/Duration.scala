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
  val durationRegex = "(\\d+)?(\\D+)".r

  def parse(expression: String): Duration =
    expression match {
      case durationRegex(null, unit) =>
        TimeUnit.lookup(unit).forValue(1)
      case durationRegex(value, unit) =>
        TimeUnit.lookup(unit).forValue(value.toInt)
      case _ =>
        throw new IllegalArgumentException(s"Cannot parse a duration from $expression.")
    }
}

case class MultiDuration(value: Int, override val unit: MultiTimeUnit) extends Duration {

  def this(unit: MultiTimeUnit) = {
    this(1, unit)
  }

  val asPeriod: Option[Period] = unit.asPeriod.map(_.multipliedBy(value))
}

object MultiDuration {

  def apply(unit: MultiTimeUnit): MultiDuration = new MultiDuration(unit)
}

case class SimpleDuration(override val unit: SimpleTimeUnit) extends Duration
