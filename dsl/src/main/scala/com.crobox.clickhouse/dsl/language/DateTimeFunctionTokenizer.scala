package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait DateTimeFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  protected def tokenizeDateTimeColumn(col: DateTimeFunctionCol[_]): String =
    col match {
      case Year(d: DateOrDateTime[_]) => s"toYear(${tokenizeColumn(d.column)})"
      case YYYYMM(d: DateOrDateTime[_]) => s"toYYYYMM(${tokenizeColumn(d.column)})"
      case Month(d: DateOrDateTime[_]) => s"toMonth(${tokenizeColumn(d.column)})"
      case DayOfMonth(d: DateOrDateTime[_]) => s"toDayOfMonth(${tokenizeColumn(d.column)})"
      case DayOfWeek(d: DateOrDateTime[_]) => s"toDayOfWeek(${tokenizeColumn(d.column)})"
      case Hour(d: DateOrDateTime[_]) => s"toHour(${tokenizeColumn(d.column)})"
      case Minute(d: DateOrDateTime[_]) => s"toMinute(${tokenizeColumn(d.column)})"
      case Second(d: DateOrDateTime[_]) => s"toSecond(${tokenizeColumn(d.column)})"
      case Monday(d: DateOrDateTime[_]) => s"toMonday(${tokenizeColumn(d.column)})"
      case AddSeconds(d: DateOrDateTime[_], seconds: NumericCol[_]) => s"addSeconds(${tokenizeColumn(d.column)},${tokenizeColumn(seconds.column)})"
      case AddMinutes(d: DateOrDateTime[_], minutes: NumericCol[_]) => s"addMinutes(${tokenizeColumn(d.column)},${tokenizeColumn(minutes.column)})"
      case AddHours(d: DateOrDateTime[_], hours: NumericCol[_]) => s"addHours(${tokenizeColumn(d.column)},${tokenizeColumn(hours.column)})"
      case AddDays(d: DateOrDateTime[_], days: NumericCol[_]) => s"addDays(${tokenizeColumn(d.column)},${tokenizeColumn(days.column)})"
      case AddWeeks(d: DateOrDateTime[_], weeks: NumericCol[_]) => s"addWeeks(${tokenizeColumn(d.column)},${tokenizeColumn(weeks.column)})"
      case AddMonths(d: DateOrDateTime[_], months: NumericCol[_]) => s"addMonths(${tokenizeColumn(d.column)},${tokenizeColumn(months.column)})"
      case AddYears(d: DateOrDateTime[_], years: NumericCol[_]) => s"addYears(${tokenizeColumn(d.column)},${tokenizeColumn(years.column)})"
      case StartOfMonth(d: DateOrDateTime[_]) => s"toStartOfMonth(${tokenizeColumn(d.column)})"
      case StartOfQuarter(d: DateOrDateTime[_]) => s"toStartOfQuarter(${tokenizeColumn(d.column)})"
      case StartOfYear(d: DateOrDateTime[_]) => s"toStartOfYear(${tokenizeColumn(d.column)})"
      case StartOfMinute(d: DateOrDateTime[_]) => s"toStartOfMinute(${tokenizeColumn(d.column)})"
      case StartOfFiveMinute(d: DateOrDateTime[_]) => s"toStartOfFiveMinute(${tokenizeColumn(d.column)})"
      case StartOfFifteenMinutes(d: DateOrDateTime[_]) => s"toStartOfFifteenMinutes(${tokenizeColumn(d.column)})"
      case StartOfHour(d: DateOrDateTime[_]) => s"toStartOfHour(${tokenizeColumn(d.column)})"
      case StartOfDay(d: DateOrDateTime[_]) => s"toStartOfDay(${tokenizeColumn(d.column)})"
      case Time(d: DateOrDateTime[_]) => s"toTime(${tokenizeColumn(d.column)})"
      case RelativeYearNum(d: DateOrDateTime[_]) => s"toRelativeYearNum(${tokenizeColumn(d.column)})"
      case RelativeQuarterNum(d: DateOrDateTime[_]) => s"toRelativeQuarterNum(${tokenizeColumn(d.column)})"
      case RelativeMonthNum(d: DateOrDateTime[_]) => s"toRelativeMonthNum(${tokenizeColumn(d.column)})"
      case RelativeWeekNum(d: DateOrDateTime[_]) => s"toRelativeWeekNum(${tokenizeColumn(d.column)})"
      case RelativeDayNum(d: DateOrDateTime[_]) => s"toRelativeDayNum(${tokenizeColumn(d.column)})"
      case RelativeHourNum(d: DateOrDateTime[_]) => s"toRelativeHourNum(${tokenizeColumn(d.column)})"
      case RelativeMinuteNum(d: DateOrDateTime[_]) => s"toRelativeMinuteNum(${tokenizeColumn(d.column)})"
      case RelativeSecondNum(d: DateOrDateTime[_]) => s"toRelativeSecondNum(${tokenizeColumn(d.column)})"
      case TimeSlot(d: DateOrDateTime[_]) => s"timeSlot(${tokenizeColumn(d.column)})"
      case TimeSlots(d: DateOrDateTime[_], duration: NumericCol[_]) => s"timeSlots(${tokenizeColumn(d.column)},${tokenizeColumn(duration.column)})"
      case ISOWeek(d: DateOrDateTime[_]) => s"toISOWeek(${tokenizeColumn(d.column)})"
      case ISOYear(d: DateOrDateTime[_]) => s"toISOYear(${tokenizeColumn(d.column)})"
      case Week(d: DateOrDateTime[_], mode: Int) => s"toWeek(${tokenizeColumn(d.column)},$mode)"
    }

  protected def tokenizeDateTimeConst(col: DateTimeConst[_]): String =
    col match {
      case Now() => s"now()"
      case Today() => s"today()"
      case Yesterday() => s"yesterday()"
    }
}