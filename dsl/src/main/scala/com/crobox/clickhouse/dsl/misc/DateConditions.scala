package com.crobox.clickhouse.dsl.misc

import com.crobox.clickhouse.dsl.ddtFromDate
import com.crobox.clickhouse.dsl.ddtFromDateCol
import com.crobox.clickhouse.dsl.numericFromLong
import com.crobox.clickhouse.dsl.numericFromLongCol
import com.crobox.clickhouse.dsl.logicalOpsMagnetFromOptionCol
import com.crobox.clickhouse.dsl.logicalOpsMagnetFromBooleanCol
import com.crobox.clickhouse.dsl.{ExpressionColumn, NativeColumn}
import org.joda.time.{DateTime, DateTimeZone, LocalDate}
import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats._

import scala.language.implicitConversions

trait DateConditions {

  /**
   * 'smart' filter function that only optionally
   * selects the timestampColumn if the startDate or endDate is not a 'full' day
   */
  def dateTimeCondition(dateColumn: NativeColumn[LocalDate],
                        timestampColumn: NativeColumn[Long],
                        startDate: DateTime,
                        endDate: Option[DateTime]): ExpressionColumn[Boolean] =
  // this
    dateColumn >= startDate.withZone(DateTimeZone.UTC).toLocalDate and
      noneIfStartOfDay(startDate).map(dt => timestampColumn >= dt.getMillis) and endDate.map(
      ed =>
        noneIfStartOfDay(ed)
          // Must be smaller equals because of current day overlap
          .map(dt => dateColumn <= dt.toLocalDate and timestampColumn < dt.getMillis)
          // Must be smaller then since endDate is not inclusive
          .getOrElse(dateColumn < ed.withZone(DateTimeZone.UTC).toLocalDate)
    )

  /**
   * 'smart' filter function that only optionally
   * selects the timestampColumn if the startDate or endDate is not a 'full' day
   */
  def dateTimeCondition(dateColumn: NativeColumn[LocalDate],
                        timestampColumn: NativeColumn[Long],
                        startDate: Option[DateTime],
                        endDate: Option[DateTime]): Option[ExpressionColumn[Boolean]] = {

    val startCondition: Option[ExpressionColumn[Boolean]] = startDate.map(sd => {
      dateColumn >= sd.withZone(DateTimeZone.UTC).toLocalDate and
        noneIfStartOfDay(sd).map(dt => timestampColumn >= dt.getMillis)
    })

    val endCondition: Option[ExpressionColumn[Boolean]] = endDate.map(ed => {
      noneIfStartOfDay(ed)
        // Must be smaller equals because of current day overlap
        .map(dt => dateColumn <= dt.toLocalDate and timestampColumn < dt.getMillis)
        // Must be smaller then since endDate is not inclusive
        .getOrElse(dateColumn < ed.withZone(DateTimeZone.UTC).toLocalDate)
    })

    startCondition match {
      case Some(condition) => Option(condition and endCondition)
      case None => endCondition
    }
  }

  def noneIfStartOfDay(dateTime: DateTime): Option[DateTime] = {
    val utcDateTime = dateTime.withZone(DateTimeZone.UTC)
    if (utcDateTime.withTimeAtStartOfDay().isEqual(utcDateTime)) {
      None
    } else {
      Some(utcDateTime)
    }
  }
}

object DateConditions extends DateConditions
