package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.dongxiguo.fastring.Fastring.Implicits._

trait DateTimeFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  protected def tokenizeDateTimeColumn(col: DateTimeFunctionCol[_]): String =
    col match {
      case Year(d: DateOrDateTime) => fast"toYear(${tokenizeColumn(d.column)})"
      case Month(d: DateOrDateTime) => fast"toMonth(${tokenizeColumn(d.column)})"
      case DayOfMonth(d: DateOrDateTime) => fast"toDayOfMonth(${tokenizeColumn(d.column)})"
      case DayOfWeek(d: DateOrDateTime) => fast"toDayOfWeek(${tokenizeColumn(d.column)})"
      case Hour(d: DateOrDateTime) => fast"toHour(${tokenizeColumn(d.column)})"
      case Minute(d: DateOrDateTime) => fast"toMinute(${tokenizeColumn(d.column)})"
      case Second(d: DateOrDateTime) => fast"toSecond(${tokenizeColumn(d.column)})"
      case Monday(d: DateOrDateTime) => fast"toMonday(${tokenizeColumn(d.column)})"
      case StartOfMonth(d: DateOrDateTime) => fast"toStartOfMonth(${tokenizeColumn(d.column)})"
      case StartOfQuarter(d: DateOrDateTime) => fast"toStartOfQuarter(${tokenizeColumn(d.column)})"
      case StartOfYear(d: DateOrDateTime) => fast"toStartOfYear(${tokenizeColumn(d.column)})"
      case StartOfMinute(d: DateOrDateTime) => fast"toStartOfMinute(${tokenizeColumn(d.column)})"
      case StartOfFiveMinute(d: DateOrDateTime) => fast"toStartOfFiveMinute(${tokenizeColumn(d.column)})"
      case StartOfFifteenMinutes(d: DateOrDateTime) => fast"toStartOfFifteenMinutes(${tokenizeColumn(d.column)})"
      case StartOfHour(d: DateOrDateTime) => fast"toStartOfHour(${tokenizeColumn(d.column)})"
      case StartOfDay(d: DateOrDateTime) => fast"toStartOfDay(${tokenizeColumn(d.column)})"
      case Time(d: DateOrDateTime) => fast"toTime(${tokenizeColumn(d.column)})"
      case RelativeYearNum(d: DateOrDateTime) => fast"toRelativeYearNum(${tokenizeColumn(d.column)})"
      case RelativeMonthNum(d: DateOrDateTime) => fast"toRelativeMonthNum(${tokenizeColumn(d.column)})"
      case RelativeWeekNum(d: DateOrDateTime) => fast"toRelativeWeekNum(${tokenizeColumn(d.column)})"
      case RelativeDayNum(d: DateOrDateTime) => fast"toRelativeDayNum(${tokenizeColumn(d.column)})"
      case RelativeHourNum(d: DateOrDateTime) => fast"toRelativeHourNum(${tokenizeColumn(d.column)})"
      case RelativeMinuteNum(d: DateOrDateTime) => fast"toRelativeMinuteNum(${tokenizeColumn(d.column)})"
      case RelativeSecondNum(d: DateOrDateTime) => fast"toRelativeSecondNum(${tokenizeColumn(d.column)})"
      case TimeSlot(d: DateOrDateTime) => fast"toTimeSlot(${tokenizeColumn(d.column)})"
      case TimeSlots(d: DateOrDateTime, duration: NumericCol) => fast"toTimeSlots(${tokenizeColumn(d.column)},${tokenizeColumn(duration.column)}"
    }

  protected def tokenizeDateTimeConst(col: DateTimeConst[_]): String =
    col match {
      case Now() => fast"now()"
      case Today() => fast"today()"
      case Yesterday() => fast"yesterday()"
    }
}