package com.crobox.clickhouse.time

import java.time.{ZoneId, ZoneOffset, ZonedDateTime}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class MultiIntervalTest extends AnyFlatSpecLike with Matchers with TableDrivenPropertyChecks {

  def toDateTime(year: Int, month: Int, day: Int, hour: Int, minutes: Int, seconds: Int, millis: Int) =
    ZonedDateTime.of(year, month, day, hour, minutes, seconds, millis * 1000000, ZoneOffset.UTC)

  "Duration parsing" should "parse expression correctly" in {
    val duration = MultiDuration(1, TimeUnit.Hour)
    Duration.parse("1h") should be(duration)
    Duration.parse("1hour") should be(duration)
    Duration.parse("h") should be(duration)
    Duration.parse("3hours") should be(MultiDuration(3, TimeUnit.Hour))
    Duration.parse("1d") should be(MultiDuration(1, TimeUnit.Day))
  }

  private val dateTime: ZonedDateTime = toDateTime(2014, 5, 8, 16, 26, 12, 123)

  "Sub intervals" should "build sub intervals and start end for all time units" in
    forAll(
      Table(
        ("Time Unit", "End interval function", "Expected intervals"),
        (
          MultiDuration(1, TimeUnit.Second),
          (time: ZonedDateTime) => time.plusSeconds(1),
          IndexedSeq(
            toDateTime(2014, 5, 8, 16, 26, 12, 0) to
              toDateTime(2014, 5, 8, 16, 26, 13, 0),
            toDateTime(2014, 5, 8, 16, 26, 13, 0) to
              toDateTime(2014, 5, 8, 16, 26, 14, 0)
          )
        ),
        (
          MultiDuration(1, TimeUnit.Minute),
          (time: ZonedDateTime) => time.plusMinutes(1),
          IndexedSeq(
            toDateTime(2014, 5, 8, 16, 26, 0, 0) to
              toDateTime(2014, 5, 8, 16, 27, 0, 0),
            toDateTime(2014, 5, 8, 16, 27, 0, 0) to
              toDateTime(2014, 5, 8, 16, 28, 0, 0)
          )
        ),
        (
          MultiDuration(1, TimeUnit.Hour),
          (time: ZonedDateTime) => time.plusHours(1),
          IndexedSeq(
            toDateTime(2014, 5, 8, 16, 0, 0, 0) to
              toDateTime(2014, 5, 8, 17, 0, 0, 0),
            toDateTime(2014, 5, 8, 17, 0, 0, 0) to
              toDateTime(2014, 5, 8, 18, 0, 0, 0)
          )
        ),
        (
          MultiDuration(6, TimeUnit.Hour),
          (time: ZonedDateTime) => time.plusHours(1),
          IndexedSeq(
            toDateTime(2014, 5, 8, 12, 0, 0, 0) to
              toDateTime(2014, 5, 8, 18, 0, 0, 0)
          )
        ),
        (
          MultiDuration(6, TimeUnit.Hour),
          (time: ZonedDateTime) => time.plusHours(6),
          IndexedSeq(
            toDateTime(2014, 5, 8, 12, 0, 0, 0) to
              toDateTime(2014, 5, 8, 18, 0, 0, 0),
            toDateTime(2014, 5, 8, 18, 0, 0, 0) to
              toDateTime(2014, 5, 9, 0, 0, 0, 0)
          )
        ),
        (
          MultiDuration(1, TimeUnit.Day),
          (time: ZonedDateTime) => time.plusDays(1),
          IndexedSeq(
            toDateTime(2014, 5, 8, 0, 0, 0, 0) to
              toDateTime(2014, 5, 9, 0, 0, 0, 0),
            toDateTime(2014, 5, 9, 0, 0, 0, 0) to
              toDateTime(2014, 5, 10, 0, 0, 0, 0)
          )
        ),
        (
          MultiDuration(1, TimeUnit.Week),
          (time: ZonedDateTime) => time.plusWeeks(1),
          IndexedSeq(
            toDateTime(2014, 5, 5, 0, 0, 0, 0) to
              toDateTime(2014, 5, 12, 0, 0, 0, 0),
            toDateTime(2014, 5, 12, 0, 0, 0, 0) to
              toDateTime(2014, 5, 19, 0, 0, 0, 0)
          )
        ),
        (
          MultiDuration(3, TimeUnit.Week),
          (time: ZonedDateTime) => time.plusWeeks(1),
          IndexedSeq(
            toDateTime(2014, 5, 5, 0, 0, 0, 0) to
              toDateTime(2014, 5, 26, 0, 0, 0, 0)
          )
        ),
        (
          MultiDuration(1, TimeUnit.Month),
          (time: ZonedDateTime) => time.plusMonths(1),
          IndexedSeq(
            toDateTime(2014, 5, 1, 0, 0, 0, 0) to
              toDateTime(2014, 6, 1, 0, 0, 0, 0),
            toDateTime(2014, 6, 1, 0, 0, 0, 0) to
              toDateTime(2014, 7, 1, 0, 0, 0, 0)
          )
        ),
        (
          MultiDuration(2, TimeUnit.Month),
          (time: ZonedDateTime) => time.plusMonths(1),
          IndexedSeq(
            toDateTime(2014, 4, 1, 0, 0, 0, 0) to
              toDateTime(2014, 6, 1, 0, 0, 0, 0),
            toDateTime(2014, 6, 1, 0, 0, 0, 0) to
              toDateTime(2014, 8, 1, 0, 0, 0, 0)
          )
        ),
        (
          MultiDuration(TimeUnit.Quarter),
          (time: ZonedDateTime) => time.plusMonths(3),
          IndexedSeq(
            toDateTime(2014, 4, 1, 0, 0, 0, 0) to
              toDateTime(2014, 7, 1, 0, 0, 0, 0),
            toDateTime(2014, 7, 1, 0, 0, 0, 0) to
              toDateTime(2014, 10, 1, 0, 0, 0, 0)
          )
        ),
        (
          MultiDuration(TimeUnit.Quarter),
          (time: ZonedDateTime) => time.plusMonths(1),
          IndexedSeq(
            toDateTime(2014, 4, 1, 0, 0, 0, 0) to
              toDateTime(2014, 7, 1, 0, 0, 0, 0)
          )
        ),
        (
          MultiDuration(TimeUnit.Year),
          (time: ZonedDateTime) => time.plusMonths(3),
          IndexedSeq(
            toDateTime(2014, 1, 1, 0, 0, 0, 0) to
              toDateTime(2015, 1, 1, 0, 0, 0, 0)
          )
        ),
        (TotalDuration, (time: ZonedDateTime) => time.plusMonths(3), IndexedSeq(dateTime to dateTime.plusMonths(3)))
      )
    ) { (duration, intervalEnd, intervals) =>
      val interval = MultiInterval(dateTime, intervalEnd(dateTime), duration)
      interval.start should be(intervals.head.start)
      interval.end should be(intervals.last.end)
      interval.subIntervals should contain theSameElementsInOrderAs intervals

    }

  it should "build correctly full time interval" in {
    val start    = ZonedDateTime.now().toLocalDate.atStartOfDay(ZoneId.systemDefault())
    val end      = start.plusDays(1)
    val interval = MultiInterval(start, end, TotalDuration)
    interval.start should be(start)
    interval.subIntervals should contain theSameElementsInOrderAs IndexedSeq(start to end)
    interval.end should be(end)
  }

  it should "include sub interval for which start date is equal to expected interval end date" in {
    val startDate     = toDateTime(2012, 1, 1, 1, 1, 9, 999)
    val endDate       = toDateTime(2012, 1, 1, 1, 1, 14, 999)
    val interval      = MultiInterval(startDate, endDate, MultiDuration(5, TimeUnit.Second))
    val startExpected = startDate.withNano(0).withSecond(5)
    interval.start should be(startExpected)
    interval.end should be(startExpected.plusSeconds(10))
    interval.subIntervals should contain theSameElementsInOrderAs ((startExpected to startExpected.plusSeconds(5)) ::
      (startExpected.plusSeconds(5) to startExpected.plusSeconds(10)) :: Nil)
  }

  // The cases above are all UTC, where MultiInterval's tzOffset arithmetic is a no-op.

  private val Amsterdam = ZoneId.of("Europe/Amsterdam")
  private val NewYork   = ZoneId.of("America/New_York")

  private def firstSubInterval(start: ZonedDateTime, duration: Duration) =
    MultiInterval(start, start.plusDays(2), duration).subIntervals.head

  "Day alignment" should "start at local midnight, east of UTC" in {
    val interval =
      firstSubInterval(ZonedDateTime.of(2014, 5, 8, 16, 26, 0, 0, Amsterdam), MultiDuration(1, TimeUnit.Day))
    interval.start should be(ZonedDateTime.of(2014, 5, 8, 0, 0, 0, 0, Amsterdam))
    interval.end should be(ZonedDateTime.of(2014, 5, 9, 0, 0, 0, 0, Amsterdam))
  }

  it should "start at local midnight, west of UTC" in {
    val interval = firstSubInterval(ZonedDateTime.of(2014, 5, 8, 16, 26, 0, 0, NewYork), MultiDuration(1, TimeUnit.Day))
    interval.start should be(ZonedDateTime.of(2014, 5, 8, 0, 0, 0, 0, NewYork))
    interval.end should be(ZonedDateTime.of(2014, 5, 9, 0, 0, 0, 0, NewYork))
  }

  it should "align a multi-day bucket to local midnight" in {
    val interval =
      firstSubInterval(ZonedDateTime.of(2014, 5, 8, 16, 26, 0, 0, Amsterdam), MultiDuration(2, TimeUnit.Day))
    interval.start should be(ZonedDateTime.of(2014, 5, 8, 0, 0, 0, 0, Amsterdam))
    interval.end should be(ZonedDateTime.of(2014, 5, 10, 0, 0, 0, 0, Amsterdam))
  }

  it should "keep a spring-forward day one calendar day, not 24 hours" in {
    val interval =
      firstSubInterval(ZonedDateTime.of(2014, 3, 30, 12, 0, 0, 0, Amsterdam), MultiDuration(1, TimeUnit.Day))
    interval.start should be(ZonedDateTime.of(2014, 3, 30, 0, 0, 0, 0, Amsterdam))
    interval.end should be(ZonedDateTime.of(2014, 3, 31, 0, 0, 0, 0, Amsterdam))
    interval.duration.toHours should be(23)
  }

  it should "keep a fall-back day one calendar day, not 24 hours" in {
    val interval =
      firstSubInterval(ZonedDateTime.of(2014, 10, 26, 12, 0, 0, 0, Amsterdam), MultiDuration(1, TimeUnit.Day))
    interval.start should be(ZonedDateTime.of(2014, 10, 26, 0, 0, 0, 0, Amsterdam))
    interval.end should be(ZonedDateTime.of(2014, 10, 27, 0, 0, 0, 0, Amsterdam))
    interval.duration.toHours should be(25)
  }

  "Week alignment" should "start on Monday at local midnight, east of UTC" in {
    val interval =
      firstSubInterval(ZonedDateTime.of(2014, 5, 8, 16, 26, 0, 0, Amsterdam), MultiDuration(1, TimeUnit.Week))
    interval.start should be(ZonedDateTime.of(2014, 5, 5, 0, 0, 0, 0, Amsterdam))
    interval.end should be(ZonedDateTime.of(2014, 5, 12, 0, 0, 0, 0, Amsterdam))
  }

  it should "start on Monday at local midnight, west of UTC" in {
    val interval =
      firstSubInterval(ZonedDateTime.of(2014, 5, 8, 16, 26, 0, 0, NewYork), MultiDuration(1, TimeUnit.Week))
    interval.start should be(ZonedDateTime.of(2014, 5, 5, 0, 0, 0, 0, NewYork))
    interval.end should be(ZonedDateTime.of(2014, 5, 12, 0, 0, 0, 0, NewYork))
  }

  it should "align a multi-week bucket to a Monday" in {
    val interval =
      firstSubInterval(ZonedDateTime.of(2014, 5, 8, 16, 26, 0, 0, Amsterdam), MultiDuration(2, TimeUnit.Week))
    interval.start should be(ZonedDateTime.of(2014, 4, 28, 0, 0, 0, 0, Amsterdam))
    interval.end should be(ZonedDateTime.of(2014, 5, 12, 0, 0, 0, 0, Amsterdam))
  }

  it should "span a DST transition inside the week" in {
    val interval =
      firstSubInterval(ZonedDateTime.of(2014, 3, 30, 12, 0, 0, 0, Amsterdam), MultiDuration(1, TimeUnit.Week))
    interval.start should be(ZonedDateTime.of(2014, 3, 24, 0, 0, 0, 0, Amsterdam))
    interval.end should be(ZonedDateTime.of(2014, 3, 31, 0, 0, 0, 0, Amsterdam))
    interval.duration.toHours should be((7 * 24) - 1)
  }

  // Pinned on purpose: unlike Day and Week above, sub-day buckets do not align to local midnight. Looks like a bug,
  // is not, and changing it would move every grouped result for non-UTC consumers.
  "Sub-day alignment" should "bucket on the epoch rather than on local midnight" in {
    val interval =
      firstSubInterval(ZonedDateTime.of(2014, 5, 8, 12, 0, 0, 0, Amsterdam), MultiDuration(6, TimeUnit.Hour))
    interval.start should be(ZonedDateTime.of(2014, 5, 8, 8, 0, 0, 0, Amsterdam))
    interval.end should be(ZonedDateTime.of(2014, 5, 8, 14, 0, 0, 0, Amsterdam))
  }

  it should "bucket minutes on the epoch, west of UTC" in {
    val interval =
      firstSubInterval(ZonedDateTime.of(2014, 5, 8, 16, 26, 12, 0, NewYork), MultiDuration(15, TimeUnit.Minute))
    interval.start should be(ZonedDateTime.of(2014, 5, 8, 16, 15, 0, 0, NewYork))
    interval.end should be(ZonedDateTime.of(2014, 5, 8, 16, 30, 0, 0, NewYork))
  }

}
