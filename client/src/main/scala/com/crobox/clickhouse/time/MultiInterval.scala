package com.crobox.clickhouse.time

import com.crobox.clickhouse.time.MultiInterval._
import com.crobox.clickhouse.time.TimeUnit._

import java.time.temporal.{ChronoUnit, TemporalAdjusters}
import java.time.{DayOfWeek, Instant, ZonedDateTime}

/**
 * A multi interval is a interval that contains subintervals, this is then used to select data by constaint, and
 * groups/aggregates this into subintervals in for example a query
 *
 * @param rawStart
 *   The starting time for the interval
 * @param rawEnd
 *   The ending time for the interval
 * @param duration
 *   The length/duration of the subintervals
 */
case class MultiInterval(rawStart: ZonedDateTime, rawEnd: ZonedDateTime, duration: Duration) {

  private val innerIntervals: IndexedSeq[TimeInterval] = intervalsBetween(rawStart, rawEnd, duration)

  val start: ZonedDateTime = startFromDate(rawStart, duration)

  val end: ZonedDateTime = innerIntervals.last.end

  def subIntervals: Seq[TimeInterval] = innerIntervals
}

object MultiInterval {

  private def atEpochMilli(reference: ZonedDateTime, millis: Long): ZonedDateTime =
    Instant.ofEpochMilli(millis).atZone(reference.getZone)

  private def startOfDay(moment: ZonedDateTime): ZonedDateTime =
    moment.toLocalDate.atStartOfDay(moment.getZone)

  private def offsetMillis(moment: ZonedDateTime): Long =
    moment.getOffset.getTotalSeconds.toLong * 1000L

  private def startFromDate(start: ZonedDateTime, duration: Duration): ZonedDateTime =
    duration match {
      case MultiDuration(value, Second) =>
        val ref = start.truncatedTo(ChronoUnit.SECONDS)

        val secs    = ref.toInstant.toEpochMilli / 1000
        val detSecs = secs - (secs % value)

        atEpochMilli(ref, detSecs * 1000)
      case MultiDuration(value, Minute) =>
        val ref = start.truncatedTo(ChronoUnit.MINUTES)

        val mins   = ref.toInstant.toEpochMilli / Minute.standardMillis
        val detMin = mins - (mins % value)

        atEpochMilli(ref, detMin * Minute.standardMillis)
      case MultiDuration(value, Hour) =>
        val ref = start.truncatedTo(ChronoUnit.HOURS)

        val hours    = ref.toInstant.toEpochMilli / Hour.standardMillis
        val detHours = hours - (hours % value)

        atEpochMilli(ref, detHours * Hour.standardMillis)
      case MultiDuration(value, Day) =>
        val ref      = startOfDay(start)
        val tzOffset = offsetMillis(ref)

        val days    = (ref.toInstant.toEpochMilli + tzOffset) / Day.standardMillis
        val detDays = days - (days % value)

        atEpochMilli(ref, (detDays * Day.standardMillis) - tzOffset)
      case MultiDuration(value, Week) =>
        val ref      = startOfDay(start).`with`(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY))
        val tzOffset = offsetMillis(ref)

        // Week 1 (since epoch) starts at the 5th of January 1970, hence we subtract the 4 days of week 0
        val msWeek1 = ref.toInstant.toEpochMilli - (Day.standardMillis * 4) + tzOffset

        val weeks    = msWeek1 / Week.standardMillis
        val detWeeks = weeks - (weeks % value)

        atEpochMilli(ref, (detWeeks * Week.standardMillis) + (Day.standardMillis * 4) - tzOffset)
      case MultiDuration(value, Month) =>
        val ref = startOfDay(start.withDayOfMonth(1))

        val months       = (ref.getYear * 12) + ref.getMonthValue
        val detRelMonths = (months - 1) - (months % value)

        val detMonthOfYearZeroBased = detRelMonths % 12
        val detYear                 = (detRelMonths - detMonthOfYearZeroBased) / 12

        ref.withYear(detYear).withMonth(detMonthOfYearZeroBased + 1)
      case MultiDuration(value, Quarter) =>
        val ref = startOfDay(start.withDayOfMonth(1))

        val quarter        = (ref.getYear * 4) + (ref.getMonthValue - 1) / 3
        val detRelQuarters = quarter - (quarter % value)

        val detQuarterOfYearZeroBased = detRelQuarters % 4
        val detYear                   = (detRelQuarters - detQuarterOfYearZeroBased) / 4

        ref.withYear(detYear).withMonth(detQuarterOfYearZeroBased * 3 + 1)

      case MultiDuration(value, Year) =>
        val ref = startOfDay(start.withDayOfYear(1))

        val detYear = ref.getYear - (ref.getYear % value)

        ref.withYear(detYear)
      case TotalDuration =>
        start
      case d => throw new IllegalArgumentException(s"Invalid duration: $d")
    }

  private def endFromDate(date: ZonedDateTime, duration: Duration): ZonedDateTime =
    duration match {
      case TotalDuration =>
        date
      case _ =>
        nextStartFromDate(startFromDate(date, duration), duration)
    }

  private def nextStartFromDate(startDate: ZonedDateTime, duration: Duration): ZonedDateTime =
    duration match {
      case d: MultiDuration => d.addTo(startDate)
      case d                => throw new IllegalArgumentException(s"Invalid duration: $d")
    }

  private def intervalsBetween(
      start: ZonedDateTime,
      end: ZonedDateTime,
      duration: Duration
  ): IndexedSeq[TimeInterval] = {
    val result = duration.unit match {
      case Total =>
        IndexedSeq(TimeInterval(start, end))
      case _ =>
        Iterator
          .iterate(TimeInterval(startFromDate(start, duration), endFromDate(start, duration))) { interval =>
            val intervalStart = nextStartFromDate(interval.start, duration)
            TimeInterval(intervalStart, endFromDate(intervalStart, duration))
          }
          .takeWhile(_.start.isBefore(end))
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
