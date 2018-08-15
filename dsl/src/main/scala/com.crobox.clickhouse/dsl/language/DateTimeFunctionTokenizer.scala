package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.dongxiguo.fastring.Fastring.Implicits._

trait DateTimeFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  protected def tokenizeDateTimeColumn(col: DateTimeFunctionCol[_]): String =
    col match {
      case Year(d: DateOrDateTime[_]) => fast"toYear(${tokenizeColumn(d.column)})"
      case YYYYMM(d: DateOrDateTime[_]) => fast"toYYYYMM(${tokenizeColumn(d.column)})"
      case Month(d: DateOrDateTime[_]) => fast"toMonth(${tokenizeColumn(d.column)})"
      case DayOfMonth(d: DateOrDateTime[_]) => fast"toDayOfMonth(${tokenizeColumn(d.column)})"
      case DayOfWeek(d: DateOrDateTime[_]) => fast"toDayOfWeek(${tokenizeColumn(d.column)})"
      case Hour(d: DateOrDateTime[_]) => fast"toHour(${tokenizeColumn(d.column)})"
      case Minute(d: DateOrDateTime[_]) => fast"toMinute(${tokenizeColumn(d.column)})"
      case Second(d: DateOrDateTime[_]) => fast"toSecond(${tokenizeColumn(d.column)})"
      case Monday(d: DateOrDateTime[_]) => fast"toMonday(${tokenizeColumn(d.column)})"
      case AddSeconds(d: DateOrDateTime[_], seconds: NumericCol[_]) => fast"addSeconds(${tokenizeColumn(d.column)},${tokenizeColumn(seconds.column)})"
      case AddMinutes(d: DateOrDateTime[_], minutes: NumericCol[_]) => fast"addMinutes(${tokenizeColumn(d.column)},${tokenizeColumn(minutes.column)})"
      case AddHours(d: DateOrDateTime[_], hours: NumericCol[_]) => fast"addHours(${tokenizeColumn(d.column)},${tokenizeColumn(hours.column)})"
      case AddDays(d: DateOrDateTime[_], days: NumericCol[_]) => fast"addDays(${tokenizeColumn(d.column)},${tokenizeColumn(days.column)})"
      case AddWeeks(d: DateOrDateTime[_], weeks: NumericCol[_]) => fast"addWeeks(${tokenizeColumn(d.column)},${tokenizeColumn(weeks.column)})"
      case AddMonths(d: DateOrDateTime[_], months: NumericCol[_]) => fast"addMonths(${tokenizeColumn(d.column)},${tokenizeColumn(months.column)})"
      case AddYears(d: DateOrDateTime[_], years: NumericCol[_]) => fast"addYears(${tokenizeColumn(d.column)},${tokenizeColumn(years.column)})"
      case StartOfMonth(d: DateOrDateTime[_]) => fast"toStartOfMonth(${tokenizeColumn(d.column)})"
      case StartOfQuarter(d: DateOrDateTime[_]) => fast"toStartOfQuarter(${tokenizeColumn(d.column)})"
      case StartOfYear(d: DateOrDateTime[_]) => fast"toStartOfYear(${tokenizeColumn(d.column)})"
      case StartOfMinute(d: DateOrDateTime[_]) => fast"toStartOfMinute(${tokenizeColumn(d.column)})"
      case StartOfFiveMinute(d: DateOrDateTime[_]) => fast"toStartOfFiveMinute(${tokenizeColumn(d.column)})"
      case StartOfFifteenMinutes(d: DateOrDateTime[_]) => fast"toStartOfFifteenMinutes(${tokenizeColumn(d.column)})"
      case StartOfHour(d: DateOrDateTime[_]) => fast"toStartOfHour(${tokenizeColumn(d.column)})"
      case StartOfDay(d: DateOrDateTime[_]) => fast"toStartOfDay(${tokenizeColumn(d.column)})"
      case Time(d: DateOrDateTime[_]) => fast"toTime(${tokenizeColumn(d.column)})"
      case RelativeYearNum(d: DateOrDateTime[_]) => fast"toRelativeYearNum(${tokenizeColumn(d.column)})"
      case RelativeMonthNum(d: DateOrDateTime[_]) => fast"toRelativeMonthNum(${tokenizeColumn(d.column)})"
      case RelativeWeekNum(d: DateOrDateTime[_]) => fast"toRelativeWeekNum(${tokenizeColumn(d.column)})"
      case RelativeDayNum(d: DateOrDateTime[_]) => fast"toRelativeDayNum(${tokenizeColumn(d.column)})"
      case RelativeHourNum(d: DateOrDateTime[_]) => fast"toRelativeHourNum(${tokenizeColumn(d.column)})"
      case RelativeMinuteNum(d: DateOrDateTime[_]) => fast"toRelativeMinuteNum(${tokenizeColumn(d.column)})"
      case RelativeSecondNum(d: DateOrDateTime[_]) => fast"toRelativeSecondNum(${tokenizeColumn(d.column)})"
      case TimeSlot(d: DateOrDateTime[_]) => fast"timeSlot(${tokenizeColumn(d.column)})"
      case TimeSlots(d: DateOrDateTime[_], duration: NumericCol[_]) => fast"timeSlots(${tokenizeColumn(d.column)},${tokenizeColumn(duration.column)})"
    }

  protected def tokenizeDateTimeConst(col: DateTimeConst[_]): String =
    col match {
      case Now() => fast"now()"
      case Today() => fast"today()"
      case Yesterday() => fast"yesterday()"
    }
}