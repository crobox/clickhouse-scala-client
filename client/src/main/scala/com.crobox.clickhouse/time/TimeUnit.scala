package com.crobox.clickhouse.time

import org.joda.time.Period

sealed trait TimeUnit {
  val labels: Array[String]
  val mainLabel: String

  def forValue(value: Int): Duration =
    this match {
      case unit: SimpleTimeUnit =>
        SimpleDuration(unit)
      case unit: MultiTimeUnit =>
        MultiDuration(value, unit)
    }
}

sealed trait FixedTimeUnit { this: TimeUnit =>
  protected[time] val standardMillis: Long
}

abstract class SimpleTimeUnit(override val labels: Array[String], override val mainLabel: String) extends TimeUnit

abstract class MultiTimeUnit(override val labels: Array[String], override val mainLabel: String) extends TimeUnit {
  val asPeriod: Period
}

/**
 * Represents a unit of time with a fixed length,
 * used in the multi interval functionality
 */
object TimeUnit {

  private lazy val allUnits =
    Seq(Second, Minute, Hour, Day, Week, Month, Quarter, Year, Total)

  case object Second extends MultiTimeUnit(Array("s", "second", "seconds"), "second") with FixedTimeUnit {
    override protected[time] val standardMillis: Long = 1000
    override val asPeriod: Period = Period.seconds(1)
  }

  case object Minute extends MultiTimeUnit(Array("m", "minute", "minutes"), "minute") with FixedTimeUnit {
    override protected[time] val standardMillis: Long = Second.standardMillis * 60
    override val asPeriod: Period = Period.minutes(1)
  }

  case object Hour extends MultiTimeUnit(Array("h", "hour", "hours"), "hour") with FixedTimeUnit {
    override protected[time] val standardMillis: Long = Minute.standardMillis * 60
    override val asPeriod: Period = Period.hours(1)
  }

  case object Day extends MultiTimeUnit(Array("d", "day", "days"), "day") with FixedTimeUnit {
    override protected[time] val standardMillis: Long = Hour.standardMillis * 24
    override val asPeriod: Period = Period.days(1)
  }

  case object Week extends MultiTimeUnit(Array("w", "week", "weeks"), "week") with FixedTimeUnit {
    override protected[time] val standardMillis: Long = Day.standardMillis * 7
    override val asPeriod: Period = Period.weeks(1)
  }

  case object Month extends MultiTimeUnit(Array("M", "month", "months"), "month") {
    override val asPeriod: Period = Period.months(1)
  }

  case object Quarter extends MultiTimeUnit(Array("q", "quarter"), "quarter"){
    override val asPeriod: Period = Period.months(3)
  }

  case object Year extends MultiTimeUnit(Array("y", "year"), "year"){
    override val asPeriod: Period = Period.years(1)
  }

  case object Total extends SimpleTimeUnit(Array("t", "total"), "total")


  def lookup(label: String): TimeUnit =
    extractUnit(label)

  def apply(period: Period): Option[TimeUnit] = period.toString match {
    case "PT1S" => Some(TimeUnit.Second)
    case "PT1M" => Some(TimeUnit.Minute)
    case "PT1H" => Some(TimeUnit.Hour)
    case "P1D"  => Some(TimeUnit.Day)
    case "P1W"  => Some(TimeUnit.Week)
    case "P1M"  => Some(TimeUnit.Month)
    case "P3M"  => Some(TimeUnit.Quarter)
    case "P1Y"  => Some(TimeUnit.Year)
    case _      => None
  }



  private[time] def extractUnit(unitLabel: String) =
    allUnits
      .find(_.labels.contains(unitLabel))
      .getOrElse(throw new IllegalArgumentException(s"Invalid label $unitLabel for time unit."))

}
