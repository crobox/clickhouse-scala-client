package com.crobox.clickhouse.time

import com.crobox.clickhouse.time.MultiInterval._
import com.crobox.clickhouse.time.TimeUnit._
import org.joda.time.base.BaseInterval
import org.joda.time.{DateTime, DateTimeConstants, Interval}

/**
 * A multi interval is a interval that contains subintervals,
 * this is then used to select data by constaint, and groups/aggregates
 * this into subintervals in for example a query
 *
 * @param rawStart The starting time for the interval
 * @param rawEnd The ending time for the interval
 * @param duration The length/duration of the subintervals
 */
case class MultiInterval(rawStart: DateTime, rawEnd: DateTime, duration: Duration)
    extends BaseInterval(startFromDate(rawStart, duration), intervalsBetween(rawStart, rawEnd, duration).last.getEnd) {

  private lazy val innerIntervals = intervalsBetween(rawStart, rawEnd, duration)

  def startOfInterval(): DateTime =
    getStart

  def endOfInterval(): DateTime =
    getEnd

  def subIntervals(): Seq[Interval] =
    innerIntervals
}

object MultiInterval {

  private def startFromDate(start: DateTime, duration: Duration) =
    duration match {
      case MultiDuration(_, Second) =>
        start.withMillisOfSecond(0)
      case MultiDuration(value, Minute) =>
        val minute = (start.getMinuteOfHour / value) * value

        start
          .withMinuteOfHour(minute)
          .withSecondOfMinute(0)
          .withMillisOfSecond(0)
      case MultiDuration(value, Hour) =>
        val hour = (start.getHourOfDay / value) * value

        start
          .withHourOfDay(hour)
          .withMinuteOfHour(0)
          .withSecondOfMinute(0)
          .withMillisOfSecond(0)
      case MultiDuration(_, Day) =>
        start.withTimeAtStartOfDay()
      case MultiDuration(_, Week) =>
        start.withTimeAtStartOfDay.withDayOfWeek(DateTimeConstants.MONDAY)
      case MultiDuration(_, Month) =>
        start.withTimeAtStartOfDay.withDayOfMonth(1)
      case MultiDuration(_, Quarter) =>
        val month = calculateQuarterStart(start.getMonthOfYear)

        start.withTimeAtStartOfDay
          .withDayOfMonth(1)
          .withMonthOfYear(month)
      case MultiDuration(_, Year) =>
        start.withTimeAtStartOfDay
          .withDayOfYear(1)
      case SimpleDuration(Total) =>
        start
      case d => throw new IllegalArgumentException(s"Invalid duration: $d")
    }

  private def calculateQuarterStart(month: Int) =
    if (month <= 3) 1
    else if (month <= 6) 4
    else if (month <= 9) 7
    else 10

  private def endFromDate(date: DateTime, duration: Duration) =
    duration match {
      case SimpleDuration(Total) =>
        date
      case _ =>
        val nextIntervalStart =
          nextStartFromDate(startFromDate(date, duration), duration)
        nextIntervalStart.minusMillis(1)
    }

  private def nextStartFromDate(startDate: DateTime, duration: Duration) =
    duration match {
      case MultiDuration(value, Second) =>
        startDate.plusSeconds(value)
      case MultiDuration(value, Minute) =>
        startDate.plusMinutes(value)
      case MultiDuration(value, Hour) =>
        startDate.plusHours(value)
      case MultiDuration(value, Day) =>
        startDate.plusDays(value)
      case MultiDuration(value, Week) =>
        startDate.plusWeeks(value)
      case MultiDuration(value, Month) =>
        startDate.plusMonths(value)
      case MultiDuration(value, Quarter) =>
        startDate.plusMonths(value * 3)
      case MultiDuration(value, Year) =>
        startDate.plusYears(value)
      case d => throw new IllegalArgumentException(s"Invalid duration: $d")
    }

  private def intervalsBetween(start: DateTime, end: DateTime, duration: Duration) = {
    val result = duration.unit match {
      case Total => IndexedSeq(new Interval(start, end))
      case _ =>
        Iterator
          .iterate(new Interval(startFromDate(start, duration), endFromDate(start, duration)))(interval => {
            val intervalStart = nextStartFromDate(interval.getStart, duration)
            new Interval(intervalStart, endFromDate(intervalStart, duration))
          })
          .takeWhile(_.getStart.isBefore(end.plusMillis(1)))
          .toIndexedSeq
    }
    if (result.isEmpty) {
      throw new IllegalArgumentException(
        s"Cannot create multi interval for start $start, end $end and duration $duration because start date is after end date."
      )
    }
    result
  }
}
