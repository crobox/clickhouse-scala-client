package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl._

trait DateTimeFunctions { self: Magnets =>
  sealed trait DateTimeFunction

  abstract class DateTimeFunctionCol[V](val ddt: DateOrDateTime)
      extends ExpressionColumn(ddt.column)
      with DateTimeFunction

  abstract class DateTimeConst[V]() extends ExpressionColumn[V](EmptyColumn()) with DateTimeFunction

  case class Year(d: DateOrDateTime)                     extends DateTimeFunctionCol[Int](d)
  case class Month(d: DateOrDateTime)                    extends DateTimeFunctionCol[Int](d)
  case class DayOfMonth(d: DateOrDateTime)               extends DateTimeFunctionCol[Int](d)
  case class DayOfWeek(d: DateOrDateTime)                extends DateTimeFunctionCol[Int](d)
  case class Hour(d: DateOrDateTime)                     extends DateTimeFunctionCol[Int](d)
  case class Minute(d: DateOrDateTime)                   extends DateTimeFunctionCol[Int](d)
  case class Second(d: DateOrDateTime)                   extends DateTimeFunctionCol[Int](d)
  case class Monday(d: DateOrDateTime)                   extends DateTimeFunctionCol[Int](d)
  case class StartOfMonth(d: DateOrDateTime)             extends DateTimeFunctionCol[Int](d)
  case class StartOfQuarter(d: DateOrDateTime)           extends DateTimeFunctionCol[Int](d)
  case class StartOfYear(d: DateOrDateTime)              extends DateTimeFunctionCol[Int](d)
  case class StartOfMinute(d: DateOrDateTime)            extends DateTimeFunctionCol[Int](d)
  case class StartOfFiveMinute(d: DateOrDateTime)        extends DateTimeFunctionCol[Int](d)
  case class StartOfFifteenMinutes(d: DateOrDateTime)    extends DateTimeFunctionCol[Int](d)
  case class StartOfHour(d: DateOrDateTime)              extends DateTimeFunctionCol[Int](d)
  case class StartOfDay(d: DateOrDateTime)               extends DateTimeFunctionCol[Int](d)
  case class Time(d: DateOrDateTime)                     extends DateTimeFunctionCol[Int](d)
  case class RelativeYearNum(d: DateOrDateTime)          extends DateTimeFunctionCol[Int](d)
  case class RelativeMonthNum(d: DateOrDateTime)         extends DateTimeFunctionCol[Int](d)
  case class RelativeWeekNum(d: DateOrDateTime)          extends DateTimeFunctionCol[Int](d)
  case class RelativeDayNum(d: DateOrDateTime)           extends DateTimeFunctionCol[Int](d)
  case class RelativeHourNum(d: DateOrDateTime)          extends DateTimeFunctionCol[Int](d)
  case class RelativeMinuteNum(d: DateOrDateTime)        extends DateTimeFunctionCol[Int](d)
  case class RelativeSecondNum(d: DateOrDateTime)        extends DateTimeFunctionCol[Int](d)
  case class Now()                                       extends DateTimeConst[Int]()
  case class Today()                                     extends DateTimeConst[Int]()
  case class Yesterday()                                 extends DateTimeConst[Int]()
  case class TimeSlot(d: DateOrDateTime)                 extends DateTimeFunctionCol[Int](d)
  case class TimeSlots(d: DateOrDateTime, duration: NumericCol) extends DateTimeFunctionCol[Int](d)

  //trait DateTimeFunctionsDsl {
  def toYear(col: DateOrDateTime)                   = Year(col)
  def toMonth(col: DateOrDateTime)                  = Month(col)
  def toDayOfMonth(col: DateOrDateTime)             = DayOfMonth(col)
  def toDayOfWeek(col: DateOrDateTime)              = DayOfWeek(col)
  def toHour(col: DateOrDateTime)                   = Hour(col)
  def toMinute(col: DateOrDateTime)                 = Minute(col)
  def toSecond(col: DateOrDateTime)                 = Second(col)
  def toMonday(col: DateOrDateTime)                 = Monday(col)
  def toStartOfMonth(col: DateOrDateTime)           = StartOfMonth(col)
  def toStartOfQuarter(col: DateOrDateTime)         = StartOfQuarter(col)
  def toStartOfYear(col: DateOrDateTime)            = StartOfYear(col)
  def toStartOfMinute(col: DateOrDateTime)          = StartOfMinute(col)
  def toStartOfFiveMinute(col: DateOrDateTime)      = StartOfFiveMinute(col)
  def toStartOfFifteenMinutes(col: DateOrDateTime)  = StartOfFifteenMinutes(col)
  def toStartOfHour(col: DateOrDateTime)            = StartOfHour(col)
  def toStartOfDay(col: DateOrDateTime)             = StartOfDay(col)
  def toTime(col: DateOrDateTime)                   = Time(col)
  def toRelativeYearNum(col: DateOrDateTime)        = RelativeYearNum(col)
  def toRelativeMonthNum(col: DateOrDateTime)       = RelativeMonthNum(col)
  def toRelativeWeekNum(col: DateOrDateTime)        = RelativeWeekNum(col)
  def toRelativeDayNum(col: DateOrDateTime)         = RelativeDayNum(col)
  def toRelativeHourNum(col: DateOrDateTime)        = RelativeHourNum(col)
  def toRelativeMinuteNum(col: DateOrDateTime)      = RelativeMinuteNum(col)
  def toRelativeSecondNum(col: DateOrDateTime)      = RelativeSecondNum(col)
  def chNow()                                       = Now()
  def chYesterday()                                 = Yesterday()
  def chToday()                                     = Today()

  def timeSlot(col: DateOrDateTime)                 = TimeSlot(col)
  def timeSlots(col: DateOrDateTime, duration: NumericCol) = TimeSlots(col, duration)
  //}
}
