package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait DateTimeFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  protected def tokenizeDateTimeColumn(col: DateTimeFunctionCol[_])(implicit ctx: TokenizeContext): String =
    col match {
      case Year(d)                => s"toYear(${tokenizeColumn(d.column)})"
      case YYYYMM(d)              => s"toYYYYMM(${tokenizeColumn(d.column)})"
      case Month(d)               => s"toMonth(${tokenizeColumn(d.column)})"
      case DayOfMonth(d)          => s"toDayOfMonth(${tokenizeColumn(d.column)})"
      case DayOfWeek(d)           => s"toDayOfWeek(${tokenizeColumn(d.column)})"
      case Hour(d)                => s"toHour(${tokenizeColumn(d.column)})"
      case Minute(d)              => s"toMinute(${tokenizeColumn(d.column)})"
      case Second(d)              => s"toSecond(${tokenizeColumn(d.column)})"
      case Monday(d)              => s"toMonday(${tokenizeColumn(d.column)})"
      case AddSeconds(d, seconds) =>
        s"addSeconds(${tokenizeColumn(d.column)},${tokenizeColumn(seconds.column)})"
      case AddMinutes(d, minutes) =>
        s"addMinutes(${tokenizeColumn(d.column)},${tokenizeColumn(minutes.column)})"
      case AddHours(d, hours) =>
        s"addHours(${tokenizeColumn(d.column)},${tokenizeColumn(hours.column)})"
      case AddDays(d, days) =>
        s"addDays(${tokenizeColumn(d.column)},${tokenizeColumn(days.column)})"
      case AddWeeks(d, weeks) =>
        s"addWeeks(${tokenizeColumn(d.column)},${tokenizeColumn(weeks.column)})"
      case AddMonths(d, months) =>
        s"addMonths(${tokenizeColumn(d.column)},${tokenizeColumn(months.column)})"
      case AddYears(d, years) =>
        s"addYears(${tokenizeColumn(d.column)},${tokenizeColumn(years.column)})"
      case StartOfMonth(d)          => s"toStartOfMonth(${tokenizeColumn(d.column)})"
      case StartOfQuarter(d)        => s"toStartOfQuarter(${tokenizeColumn(d.column)})"
      case StartOfYear(d)           => s"toStartOfYear(${tokenizeColumn(d.column)})"
      case StartOfMinute(d)         => s"toStartOfMinute(${tokenizeColumn(d.column)})"
      case StartOfFiveMinute(d)     => s"toStartOfFiveMinute(${tokenizeColumn(d.column)})"
      case StartOfFifteenMinutes(d) => s"toStartOfFifteenMinutes(${tokenizeColumn(d.column)})"
      case StartOfHour(d)           => s"toStartOfHour(${tokenizeColumn(d.column)})"
      case StartOfDay(d)            => s"toStartOfDay(${tokenizeColumn(d.column)})"
      case Time(d)                  => s"toTime(${tokenizeColumn(d.column)})"
      case RelativeYearNum(d)       => s"toRelativeYearNum(${tokenizeColumn(d.column)})"
      case RelativeQuarterNum(d)    => s"toRelativeQuarterNum(${tokenizeColumn(d.column)})"
      case RelativeMonthNum(d)      => s"toRelativeMonthNum(${tokenizeColumn(d.column)})"
      case RelativeWeekNum(d)       => s"toRelativeWeekNum(${tokenizeColumn(d.column)})"
      case RelativeDayNum(d)        => s"toRelativeDayNum(${tokenizeColumn(d.column)})"
      case RelativeHourNum(d)       => s"toRelativeHourNum(${tokenizeColumn(d.column)})"
      case RelativeMinuteNum(d)     => s"toRelativeMinuteNum(${tokenizeColumn(d.column)})"
      case RelativeSecondNum(d)     => s"toRelativeSecondNum(${tokenizeColumn(d.column)})"
      case TimeSlot(d)              => s"timeSlot(${tokenizeColumn(d.column)})"
      case TimeSlots(d, duration)   =>
        s"timeSlots(${tokenizeColumn(d.column)},${tokenizeColumn(duration.column)})"
      case ISOWeek(d)    => s"toISOWeek(${tokenizeColumn(d.column)})"
      case ISOYear(d)    => s"toISOYear(${tokenizeColumn(d.column)})"
      case Week(d, mode) => s"toWeek(${tokenizeColumn(d.column)},$mode)"
    }

  protected def tokenizeDateTimeConst(col: DateTimeConst[_]): String =
    col match {
      case Now()       => s"now()"
      case Today()     => s"today()"
      case Yesterday() => s"yesterday()"
    }
}
