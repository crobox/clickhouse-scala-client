package com.crobox.clickhouse.dsl.misc

import com.crobox.clickhouse.dsl.ddtFromDate
import com.crobox.clickhouse.dsl.ddtFromDateCol
import com.crobox.clickhouse.dsl.numericFromLong
import com.crobox.clickhouse.dsl.numericFromLongCol
import com.crobox.clickhouse.dsl.logicalOpsMagnetFromOptionCol
import com.crobox.clickhouse.dsl.logicalOpsMagnetFromBooleanCol
import com.crobox.clickhouse.dsl.{ExpressionColumn, NativeColumn}
import java.time.{LocalDate, ZoneOffset, ZonedDateTime}
import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats._

import scala.language.implicitConversions

trait DateConditions {

  /**
   * 'smart' filter function that only optionally selects the timestampColumn if the startDate or endDate is not a
   * 'full' day
   */
  def dateTimeCondition(
      dateColumn: NativeColumn[LocalDate],
      timestampColumn: NativeColumn[Long],
      startDate: ZonedDateTime,
      endDate: Option[ZonedDateTime]
  ): ExpressionColumn[Boolean] =
    // this
    dateColumn >= startDate.withZoneSameInstant(ZoneOffset.UTC).toLocalDate and
      noneIfStartOfDay(startDate).map(dt => timestampColumn >= dt.toInstant.toEpochMilli) and endDate.map(ed =>
        noneIfStartOfDay(ed)
          // Must be smaller equals because of current day overlap
          .map(dt => dateColumn <= dt.toLocalDate and timestampColumn < dt.toInstant.toEpochMilli)
          // Must be smaller then since endDate is not inclusive
          .getOrElse(dateColumn < ed.withZoneSameInstant(ZoneOffset.UTC).toLocalDate)
      )

  /**
   * 'smart' filter function that only optionally selects the timestampColumn if the startDate or endDate is not a
   * 'full' day
   */
  def dateTimeCondition(
      dateColumn: NativeColumn[LocalDate],
      timestampColumn: NativeColumn[Long],
      startDate: Option[ZonedDateTime],
      endDate: Option[ZonedDateTime]
  ): Option[ExpressionColumn[Boolean]] = {

    val startCondition: Option[ExpressionColumn[Boolean]] = startDate.map(sd =>
      dateColumn >= sd.withZoneSameInstant(ZoneOffset.UTC).toLocalDate and
        noneIfStartOfDay(sd).map(dt => timestampColumn >= dt.toInstant.toEpochMilli)
    )

    val endCondition: Option[ExpressionColumn[Boolean]] = endDate.map(ed =>
      noneIfStartOfDay(ed)
        // Must be smaller equals because of current day overlap
        .map(dt => dateColumn <= dt.toLocalDate and timestampColumn < dt.toInstant.toEpochMilli)
        // Must be smaller then since endDate is not inclusive
        .getOrElse(dateColumn < ed.withZoneSameInstant(ZoneOffset.UTC).toLocalDate)
    )

    startCondition match {
      case Some(condition) => Option(condition and endCondition)
      case None            => endCondition
    }
  }

  def noneIfStartOfDay(dateTime: ZonedDateTime): Option[ZonedDateTime] = {
    val utcDateTime = dateTime.withZoneSameInstant(ZoneOffset.UTC)
    if (utcDateTime.toLocalDate.atStartOfDay(ZoneOffset.UTC).isEqual(utcDateTime)) {
      None
    } else {
      Some(utcDateTime)
    }
  }
}

object DateConditions extends DateConditions
