package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl._
import org.joda.time.{DateTime, LocalDate}

trait DateTimeFunctions { self: Magnets =>
  sealed trait DateTimeFunction

  abstract class DateTimeFunctionCol[V](val ddt: DateOrDateTime)
      extends ExpressionColumn(ddt.column)
      with DateTimeFunction

  abstract class DateTimeConst[V]() extends ExpressionColumn[V](EmptyColumn()) with DateTimeFunction

  case class Year(d: DateOrDateTime)                     extends DateTimeFunctionCol[Int](d)
  case class YYYYMM(d: DateOrDateTime)                   extends DateTimeFunctionCol[String](d)
  case class Month(d: DateOrDateTime)                    extends DateTimeFunctionCol[Int](d)
  case class DayOfMonth(d: DateOrDateTime)               extends DateTimeFunctionCol[Int](d)
  case class DayOfWeek(d: DateOrDateTime)                extends DateTimeFunctionCol[Int](d)
  case class Hour(d: DateOrDateTime)                     extends DateTimeFunctionCol[Int](d)
  case class Minute(d: DateOrDateTime)                   extends DateTimeFunctionCol[Int](d)
  case class Second(d: DateOrDateTime)                   extends DateTimeFunctionCol[Int](d)
  case class Monday[V](d: DateOrDateTime)                extends DateTimeFunctionCol[V](d)
  case class StartOfMonth[V](d: DateOrDateTime)          extends DateTimeFunctionCol[V](d)
  case class StartOfQuarter[V](d: DateOrDateTime)           extends DateTimeFunctionCol[V](d)
  case class StartOfYear[V](d: DateOrDateTime)              extends DateTimeFunctionCol[V](d)
  case class StartOfMinute[V](d: DateOrDateTime)            extends DateTimeFunctionCol[V](d)
  case class StartOfFiveMinute[V](d: DateOrDateTime)        extends DateTimeFunctionCol[V](d)
  case class StartOfFifteenMinutes[V](d: DateOrDateTime)    extends DateTimeFunctionCol[V](d)
  case class StartOfHour[V](d: DateOrDateTime)              extends DateTimeFunctionCol[V](d)
  case class StartOfDay[V](d: DateOrDateTime)               extends DateTimeFunctionCol[V](d)
  case class Time(d: DateOrDateTime)                        extends DateTimeFunctionCol[DateTime](d)
  case class RelativeYearNum[V](d: DateOrDateTime)          extends DateTimeFunctionCol[V](d)
  case class RelativeMonthNum[V](d: DateOrDateTime)         extends DateTimeFunctionCol[V](d)
  case class RelativeWeekNum[V](d: DateOrDateTime)          extends DateTimeFunctionCol[V](d)
  case class RelativeDayNum[V](d: DateOrDateTime)           extends DateTimeFunctionCol[V](d)
  case class RelativeHourNum[V](d: DateOrDateTime)          extends DateTimeFunctionCol[V](d)
  case class RelativeMinuteNum[V](d: DateOrDateTime)        extends DateTimeFunctionCol[V](d)
  case class RelativeSecondNum[V](d: DateOrDateTime)        extends DateTimeFunctionCol[V](d)
  case class Now()                                       extends DateTimeConst[DateTime]()
  case class Today()                                     extends DateTimeConst[LocalDate]()
  case class Yesterday()                                 extends DateTimeConst[LocalDate]()
  case class TimeSlot(d: DateOrDateTime)                 extends DateTimeFunctionCol[DateTime](d)
  case class TimeSlots(d: DateOrDateTime, duration: NumericCol) extends DateTimeFunctionCol[DateTime](d)

  //trait DateTimeFunctionsDsl {
  def toYear(col: DateOrDateTime)                   = Year(col)
  def toYYYYMM(col: DateOrDateTime)                 = YYYYMM(col)
  def toMonth(col: DateOrDateTime)                  = Month(col)
  def toDayOfMonth(col: DateOrDateTime)             = DayOfMonth(col)
  def toDayOfWeek(col: DateOrDateTime)              = DayOfWeek(col)
  def toHour(col: DateOrDateTime)                   = Hour(col)
  def toMinute(col: DateOrDateTime)                 = Minute(col)
  def toSecond(col: DateOrDateTime)                 = Second(col)
  def toMonday(col: DateOrDateTime)                 = Monday[col.ColumnType](col)
  def toStartOfMonth(col: DateOrDateTime)           = StartOfMonth[col.ColumnType](col)
  def toStartOfQuarter(col: DateOrDateTime)         = StartOfQuarter[col.ColumnType](col)
  def toStartOfYear(col: DateOrDateTime)            = StartOfYear[col.ColumnType](col)
  def toStartOfMinute(col: DateOrDateTime)          = StartOfMinute[col.ColumnType](col)
  def toStartOfFiveMinute(col: DateOrDateTime)      = StartOfFiveMinute[col.ColumnType](col)
  def toStartOfFifteenMinutes(col: DateOrDateTime)  = StartOfFifteenMinutes[col.ColumnType](col)
  def toStartOfHour(col: DateOrDateTime)            = StartOfHour[col.ColumnType](col)
  def toStartOfDay(col: DateOrDateTime)             = StartOfDay[col.ColumnType](col)
  def toTime(col: DateOrDateTime)                   = Time(col)
  def toRelativeYearNum(col: DateOrDateTime)        = RelativeYearNum[col.ColumnType](col)
  def toRelativeMonthNum(col: DateOrDateTime)       = RelativeMonthNum[col.ColumnType](col)
  def toRelativeWeekNum(col: DateOrDateTime)        = RelativeWeekNum[col.ColumnType](col)
  def toRelativeDayNum(col: DateOrDateTime)         = RelativeDayNum[col.ColumnType](col)
  def toRelativeHourNum(col: DateOrDateTime)        = RelativeHourNum[col.ColumnType](col)
  def toRelativeMinuteNum(col: DateOrDateTime)      = RelativeMinuteNum[col.ColumnType](col)
  def toRelativeSecondNum(col: DateOrDateTime)      = RelativeSecondNum[col.ColumnType](col)
  def chNow()                                       = Now()
  def chYesterday()                                 = Yesterday()
  def chToday()                                     = Today()

  def timeSlot(col: DateOrDateTime)                 = TimeSlot(col)
  def timeSlots(col: DateOrDateTime, duration: NumericCol) = TimeSlots(col, duration)
  //}
}
