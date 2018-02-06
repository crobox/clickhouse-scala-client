package com.crobox.clickhouse.partitioning

import java.util.Date

import org.joda.time.{DateTime, LocalDate}
import org.joda.time.format.DateTimeFormat

object PartitionDateFormatter {
  private val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")

  def dateFormat(ts: Long): String = formatter.print(ts)

  def dateFormat(dt: LocalDate): String = formatter.print(dt)

  def dateFormat(dt: DateTime): String = dateFormat(dt.getMillis)

  def dateFormat(dt: Date): String = dateFormat(dt.getTime)
}
