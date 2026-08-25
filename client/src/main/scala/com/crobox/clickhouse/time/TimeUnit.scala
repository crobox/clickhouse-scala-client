package com.crobox.clickhouse.time

import java.time.temporal.ChronoUnit

sealed trait TimeUnit {
  val labels: Array[String]
  val mainLabel: String

  def forValue(value: Int): Duration =
    this match {
      case TimeUnit.Total =>
        TotalDuration
      case unit: MultiTimeUnit =>
        MultiDuration(value, unit)
    }
}

/**
 * @param amount
 *   how many `chronoUnit`s one of this unit spans, so a quarter is three months.
 */
abstract class MultiTimeUnit(
    override val labels: Array[String],
    override val mainLabel: String,
    val amount: Int,
    val chronoUnit: ChronoUnit
) extends TimeUnit {

  /** False for month and longer, whose length depends on the calendar. */
  def hasFixedLength: Boolean = true

  // Refuses month and longer rather than returning java.time's ~30.44-day estimate for them, which is what joda's
  // Period.toStandardDuration did. MultiInterval never asks: it aligns those units by calendar field.
  lazy protected[time] val standardMillis: Long =
    if (hasFixedLength) chronoUnit.getDuration.toMillis * amount
    else
      throw new UnsupportedOperationException(
        s"Cannot convert a $mainLabel to milliseconds, because months and longer vary in length"
      )
}

/**
 * Represents a unit of time with a fixed length, used in the multi interval functionality
 */
object TimeUnit {

  private lazy val allUnits =
    Seq(Second, Minute, Hour, Day, Week, Month, Quarter, Year, Total)

  case object Second extends MultiTimeUnit(Array("s", "second", "seconds"), "second", 1, ChronoUnit.SECONDS)

  case object Minute extends MultiTimeUnit(Array("m", "minute", "minutes"), "minute", 1, ChronoUnit.MINUTES)

  case object Hour extends MultiTimeUnit(Array("h", "hour", "hours"), "hour", 1, ChronoUnit.HOURS)

  case object Day extends MultiTimeUnit(Array("d", "day", "days"), "day", 1, ChronoUnit.DAYS)

  case object Week extends MultiTimeUnit(Array("w", "week", "weeks"), "week", 1, ChronoUnit.WEEKS)

  case object Month extends MultiTimeUnit(Array("M", "month", "months"), "month", 1, ChronoUnit.MONTHS) {
    override def hasFixedLength: Boolean = false
  }

  case object Quarter extends MultiTimeUnit(Array("q", "quarter"), "quarter", 3, ChronoUnit.MONTHS) {
    override def hasFixedLength: Boolean = false
  }

  case object Year extends MultiTimeUnit(Array("y", "year"), "year", 1, ChronoUnit.YEARS) {
    override def hasFixedLength: Boolean = false
  }

  case object Total extends TimeUnit {
    override val labels: Array[String] = Array("t", "total")
    override val mainLabel: String     = "total"
  }

  def lookup(label: String): TimeUnit = allUnits
    .find(_.labels.contains(label))
    .getOrElse(throw new IllegalArgumentException(s"Invalid label $label for time unit."))

  def apply(amount: Int, chronoUnit: ChronoUnit): Option[TimeUnit] =
    allUnits.collectFirst {
      case unit: MultiTimeUnit if unit.amount == amount && unit.chronoUnit == chronoUnit => unit
    }
}
