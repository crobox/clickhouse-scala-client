package com.crobox.clickhouse.time

import com.crobox.clickhouse.time.MultiInterval._
import com.crobox.clickhouse.time.TimeUnit._
import org.joda.time.base.BaseInterval
import org.joda.time.{DateTime, DateTimeConstants, DateTimeZone, Interval}

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
      case MultiDuration(value, Second) =>
        val ref = start.withMillisOfSecond(0)

        val secs = ref.getMillis / 1000
        val detSecs = secs - (secs % value)

        ref.withMillis(detSecs * 1000)
      case MultiDuration(value, Minute) =>
        val ref = start
          .withSecondOfMinute(0)
          .withMillisOfSecond(0)


        val mins = ref.getMillis / Minute.standardMillis
        val detMin = mins - (mins % value)

        ref.withMillis(detMin * Minute.standardMillis)
      case MultiDuration(value, Hour) =>
        val ref = start
          .withMinuteOfHour(0)
          .withSecondOfMinute(0)
          .withMillisOfSecond(0)

        val hours = ref.getMillis / Hour.standardMillis
        val detHours = hours - (hours % value)

        ref.withMillis(detHours * Hour.standardMillis)
      case MultiDuration(value, Day) =>
        val ref = start.withTimeAtStartOfDay()
        val tzOffset = ref.getZone.getOffset(ref.withZone(DateTimeZone.UTC))

        val days = (ref.getMillis + tzOffset) / Day.standardMillis
        val detDays = days - (days % value)

        ref.withMillis((detDays * Day.standardMillis) - tzOffset)
      case MultiDuration(value, Week) =>
        val ref = start.withTimeAtStartOfDay.withDayOfWeek(DateTimeConstants.MONDAY)
        val tzOffset = ref.getZone.getOffset(ref.withZone(DateTimeZone.UTC))

        //Week 1 (since epoch) starts at the 5th of January 1970, hence we subtract the 4 days of week 0
        val msWeek1 = ref.getMillis - (Day.standardMillis * 4) + tzOffset

        val weeks = msWeek1 / Week.standardMillis
        val detWeeks = weeks - (weeks % value)

        ref.withMillis((detWeeks * Week.standardMillis) + (Day.standardMillis * 4) - tzOffset)
      case MultiDuration(value, Month) =>
        val ref = start.withTimeAtStartOfDay.withDayOfMonth(1)

        val months = (ref.getYear * 12) + ref.getMonthOfYear
        val detRelMonths = (months - 1) - (months % value)

        val detMonthOfYearZeroBased = detRelMonths % 12
        val detYear = (detRelMonths - detMonthOfYearZeroBased) / 12

        ref.withYear(detYear).withMonthOfYear(detMonthOfYearZeroBased + 1)
      case MultiDuration(value, Quarter) =>
        val ref = start.withTimeAtStartOfDay.withDayOfMonth(1)

        val months = (ref.getYear * 12) + ref.getMonthOfYear
        val detRelMonths = (months - 1) - (months % (value*3))

        val detMonthOfYearZeroBased = detRelMonths % 12
        val detYear = (detRelMonths - detMonthOfYearZeroBased) / 12

        ref.withYear(detYear).withMonthOfYear(detMonthOfYearZeroBased + 1)
      case MultiDuration(value, Year) =>
        val ref = start.withTimeAtStartOfDay
          .withDayOfYear(1)

        val detYear = ref.getYear - (ref.getYear % value)

        ref.withYear(detYear)
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
