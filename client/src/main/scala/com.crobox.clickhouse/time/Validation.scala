package com.crobox.clickhouse.time

import com.typesafe.config.Config

import scala.collection.JavaConverters._

trait DurationValidator {

  def config: Config

  private lazy val validUnits: Seq[String] =
    config.getStringList(s"${DurationValidator.ConfigurationPrefix}.units").asScala

  def validate(duration: Duration): Unit =
    duration match {
      case MultiDuration(value, unit) =>
        if (!validUnits.contains(unit.mainLabel)) {
          throw new IllegalArgumentException(
            s"The unit ${unit.mainLabel} is not valid for configuration. Only units $validUnits are accepted."
          )
        }
        if (!config
              .getIntList(s"${DurationValidator.ConfigurationPrefix}.unit.${unit.mainLabel}")
              .asScala
              .map(_.toInt)
              .contains(value))
          throw new IllegalArgumentException(s"Cannot use unit $unit with value $value.")
      case _ =>
    }

}

object DurationValidator {
  val ConfigurationPrefix = "crobox.time.multi-interval"
}

trait MultiIntervalValidator {

  def config: Config

  private lazy val maximumGranularity: Int =
    config.getInt(s"${DurationValidator.ConfigurationPrefix}.maximum-sub-intervals")

  def validate(interval: MultiInterval): Unit =
    if (interval.subIntervals().length > maximumGranularity) {
      throw new IllegalArgumentException(
        s"Specified granularity is too small. It would produce a number of ${interval.subIntervals().length} sub intervals, while " +
        s"only a maximum of $maximumGranularity is allowed."
      )
    }
}
