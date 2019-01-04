package com.crobox.clickhouse.time

import org.joda.time.Period

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

abstract class MultiTimeUnit(override val labels: Array[String], override val mainLabel: String) extends TimeUnit {
  val asPeriod: Period
  lazy protected[time] val standardMillis: Long = asPeriod.toStandardDuration.getMillis
}

/**
 * Represents a unit of time with a fixed length,
 * used in the multi interval functionality
 */
object TimeUnit {

  private lazy val allUnits =
    Seq(Second, Minute, Hour, Day, Week, Month, Quarter, Year, Total)

  case object Second extends MultiTimeUnit(Array("s", "second", "seconds"), "second") {
    override val asPeriod: Period = Period.seconds(1)
  }

  case object Minute extends MultiTimeUnit(Array("m", "minute", "minutes"), "minute") {
    override val asPeriod: Period = Period.minutes(1)
  }

  case object Hour extends MultiTimeUnit(Array("h", "hour", "hours"), "hour") {
    override val asPeriod: Period = Period.hours(1)
  }

  case object Day extends MultiTimeUnit(Array("d", "day", "days"), "day") {
    override val asPeriod: Period = Period.days(1)
  }

  case object Week extends MultiTimeUnit(Array("w", "week", "weeks"), "week") {
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

  case object Total extends TimeUnit {
    override val labels: Array[String] = Array("t", "total")
    override val mainLabel: String = "total"
  }


  def lookup(label: String): TimeUnit = allUnits
    .find(_.labels.contains(label))
    .getOrElse(throw new IllegalArgumentException(s"Invalid label $label for time unit."))

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

}
