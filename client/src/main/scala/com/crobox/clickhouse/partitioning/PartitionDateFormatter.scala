package com.crobox.clickhouse.partitioning

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, ZoneId, ZonedDateTime}
import java.util.Date

object PartitionDateFormatter {
  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  /** A bare instant carries no zone, so the system default decides the date, as it did before. */
  def dateFormat(ts: Long): String =
    formatter.format(Instant.ofEpochMilli(ts).atZone(ZoneId.systemDefault()))

  def dateFormat(dt: LocalDate): String = formatter.format(dt)

  /**
   * Uses the value's own zone. Previously this went through the epoch millis and so used the system default instead.
   */
  def dateFormat(dt: ZonedDateTime): String = formatter.format(dt)

  def dateFormat(dt: Date): String = dateFormat(dt.getTime)
}
