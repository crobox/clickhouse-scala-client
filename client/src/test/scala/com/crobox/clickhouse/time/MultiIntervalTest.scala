package com.crobox.clickhouse.time

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.DateTimeZone.UTC
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class MultiIntervalTest extends AnyFlatSpecLike with Matchers with TableDrivenPropertyChecks {

  def toDateTime(year: Int, month: Int, day: Int, hour: Int, minutes: Int, seconds: Int, millis: Int) =
    new DateTime(year, month, day, hour, minutes, seconds, millis, UTC)

  "Duration parsing" should "parse expression correctly" in {
    val duration = MultiDuration(1, TimeUnit.Hour)
    Duration.parse("1h") should be(duration)
    Duration.parse("1hour") should be(duration)
    Duration.parse("h") should be(duration)
    Duration.parse("3hours") should be(MultiDuration(3, TimeUnit.Hour))
    Duration.parse("1d") should be(MultiDuration(1, TimeUnit.Day))
  }

  private val dateTime: DateTime = toDateTime(2014, 5, 8, 16, 26, 12, 123)

  "Sub intervals" should "build sub intervals and start end for all time units" in
    forAll(
      Table(
        ("Time Unit", "End interval function", "Expected intervals"),
        (
          MultiDuration(1, TimeUnit.Second),
          (time: DateTime) => time.plusSeconds(1),
          IndexedSeq(
            toDateTime(2014, 5, 8, 16, 26, 12, 0) to
              toDateTime(2014, 5, 8, 16, 26, 13, 0),
            toDateTime(2014, 5, 8, 16, 26, 13, 0) to
              toDateTime(2014, 5, 8, 16, 26, 14, 0)
          )
        ),
        (
          MultiDuration(1, TimeUnit.Minute),
          (time: DateTime) => time.plusMinutes(1),
          IndexedSeq(
            toDateTime(2014, 5, 8, 16, 26, 0, 0) to
              toDateTime(2014, 5, 8, 16, 27, 0, 0),
            toDateTime(2014, 5, 8, 16, 27, 0, 0) to
              toDateTime(2014, 5, 8, 16, 28, 0, 0)
          )
        ),
        (
          MultiDuration(1, TimeUnit.Hour),
          (time: DateTime) => time.plusHours(1),
          IndexedSeq(
            toDateTime(2014, 5, 8, 16, 0, 0, 0) to
              toDateTime(2014, 5, 8, 17, 0, 0, 0),
            toDateTime(2014, 5, 8, 17, 0, 0, 0) to
              toDateTime(2014, 5, 8, 18, 0, 0, 0)
          )
        ),
        (
          MultiDuration(6, TimeUnit.Hour),
          (time: DateTime) => time.plusHours(1),
          IndexedSeq(
            toDateTime(2014, 5, 8, 12, 0, 0, 0) to
              toDateTime(2014, 5, 8, 18, 0, 0, 0)
          )
        ),
        (
          MultiDuration(6, TimeUnit.Hour),
          (time: DateTime) => time.plusHours(6),
          IndexedSeq(
            toDateTime(2014, 5, 8, 12, 0, 0, 0) to
              toDateTime(2014, 5, 8, 18, 0, 0, 0),
            toDateTime(2014, 5, 8, 18, 0, 0, 0) to
              toDateTime(2014, 5, 9, 0, 0, 0, 0)
          )
        ),
        (
          MultiDuration(1, TimeUnit.Day),
          (time: DateTime) => time.plusDays(1),
          IndexedSeq(
            toDateTime(2014, 5, 8, 0, 0, 0, 0) to
              toDateTime(2014, 5, 9, 0, 0, 0, 0),
            toDateTime(2014, 5, 9, 0, 0, 0, 0) to
              toDateTime(2014, 5, 10, 0, 0, 0, 0)
          )
        ),
        (
          MultiDuration(1, TimeUnit.Week),
          (time: DateTime) => time.plusWeeks(1),
          IndexedSeq(
            toDateTime(2014, 5, 5, 0, 0, 0, 0) to
              toDateTime(2014, 5, 12, 0, 0, 0, 0),
            toDateTime(2014, 5, 12, 0, 0, 0, 0) to
              toDateTime(2014, 5, 19, 0, 0, 0, 0)
          )
        ),
        (
          MultiDuration(3, TimeUnit.Week),
          (time: DateTime) => time.plusWeeks(1),
          IndexedSeq(
            toDateTime(2014, 5, 5, 0, 0, 0, 0) to
              toDateTime(2014, 5, 26, 0, 0, 0, 0)
          )
        ),
        (
          MultiDuration(1, TimeUnit.Month),
          (time: DateTime) => time.plusMonths(1),
          IndexedSeq(
            toDateTime(2014, 5, 1, 0, 0, 0, 0) to
              toDateTime(2014, 6, 1, 0, 0, 0, 0),
            toDateTime(2014, 6, 1, 0, 0, 0, 0) to
              toDateTime(2014, 7, 1, 0, 0, 0, 0)
          )
        ),
        (
          MultiDuration(2, TimeUnit.Month),
          (time: DateTime) => time.plusMonths(1),
          IndexedSeq(
            toDateTime(2014, 4, 1, 0, 0, 0, 0) to
              toDateTime(2014, 6, 1, 0, 0, 0, 0),
            toDateTime(2014, 6, 1, 0, 0, 0, 0) to
              toDateTime(2014, 8, 1, 0, 0, 0, 0)
          )
        ),
        (
          MultiDuration(TimeUnit.Quarter),
          (time: DateTime) => time.plusMonths(3),
          IndexedSeq(
            toDateTime(2014, 4, 1, 0, 0, 0, 0) to
              toDateTime(2014, 7, 1, 0, 0, 0, 0),
            toDateTime(2014, 7, 1, 0, 0, 0, 0) to
              toDateTime(2014, 10, 1, 0, 0, 0, 0)
          )
        ),
        (
          MultiDuration(TimeUnit.Quarter),
          (time: DateTime) => time.plusMonths(1),
          IndexedSeq(
            toDateTime(2014, 4, 1, 0, 0, 0, 0) to
              toDateTime(2014, 7, 1, 0, 0, 0, 0)
          )
        ),
        (
          MultiDuration(TimeUnit.Year),
          (time: DateTime) => time.plusMonths(3),
          IndexedSeq(
            toDateTime(2014, 1, 1, 0, 0, 0, 0) to
              toDateTime(2015, 1, 1, 0, 0, 0, 0)
          )
        ),
        (TotalDuration, (time: DateTime) => time.plusMonths(3), IndexedSeq(dateTime to dateTime.plusMonths(3)))
      )
    ) { (duration, intervalEnd, intervals) =>
      val interval = MultiInterval(dateTime, intervalEnd(dateTime), duration)
      interval.getStart should be(intervals.head.getStart)
      interval.getEnd should be(intervals.last.getEnd)
      interval.subIntervals should contain theSameElementsInOrderAs intervals

    }

  it should "build correctly full time interval" in {
    val start    = DateTime.now().withTimeAtStartOfDay()
    val end      = start.plusDays(1)
    val interval = MultiInterval(start, end, TotalDuration)
    interval.getStart should be(start)
    interval.subIntervals should contain theSameElementsInOrderAs IndexedSeq(start to end)
    interval.getEnd should be(end)
  }

  it should "include sub interval for which start date is equal to expected interval end date" in {
    val startDate     = toDateTime(2012, 1, 1, 1, 1, 9, 999)
    val endDate       = toDateTime(2012, 1, 1, 1, 1, 14, 999)
    val interval      = MultiInterval(startDate, endDate, MultiDuration(5, TimeUnit.Second))
    val startExpected = startDate.withMillisOfSecond(0).withSecondOfMinute(5)
    interval.getStart should be(startExpected)
    interval.getEnd should be(startExpected.plusSeconds(10))
    interval.subIntervals should contain theSameElementsInOrderAs ((startExpected to startExpected.plusSeconds(5)) ::
      (startExpected.plusSeconds(5) to startExpected.plusSeconds(10)) :: Nil)
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Zone-sensitive alignment.
  //
  // Everything above runs in UTC, where the `tzOffset` arithmetic in MultiInterval's Day and Week branches is a
  // no-op -- so none of it was exercised. These pin the current behaviour in real zones, on both sides of UTC and
  // across both DST transitions, because that behaviour is what a change of date library is most likely to alter
  // quietly.
  // ---------------------------------------------------------------------------------------------------------------

  private val Amsterdam = DateTimeZone.forID("Europe/Amsterdam")
  private val NewYork   = DateTimeZone.forID("America/New_York")

  private def firstSubInterval(start: DateTime, duration: Duration) =
    MultiInterval(start, start.plusDays(2), duration).subIntervals.head

  "Day alignment" should "start at local midnight, east of UTC" in {
    val interval = firstSubInterval(new DateTime(2014, 5, 8, 16, 26, 0, Amsterdam), MultiDuration(1, TimeUnit.Day))
    interval.getStart should be(new DateTime(2014, 5, 8, 0, 0, 0, Amsterdam))
    interval.getEnd should be(new DateTime(2014, 5, 9, 0, 0, 0, Amsterdam))
  }

  it should "start at local midnight, west of UTC" in {
    val interval = firstSubInterval(new DateTime(2014, 5, 8, 16, 26, 0, NewYork), MultiDuration(1, TimeUnit.Day))
    interval.getStart should be(new DateTime(2014, 5, 8, 0, 0, 0, NewYork))
    interval.getEnd should be(new DateTime(2014, 5, 9, 0, 0, 0, NewYork))
  }

  it should "align a multi-day bucket to local midnight" in {
    val interval = firstSubInterval(new DateTime(2014, 5, 8, 16, 26, 0, Amsterdam), MultiDuration(2, TimeUnit.Day))
    interval.getStart should be(new DateTime(2014, 5, 8, 0, 0, 0, Amsterdam))
    interval.getEnd should be(new DateTime(2014, 5, 10, 0, 0, 0, Amsterdam))
  }

  // The clocks go forward at 02:00, so this calendar day is 23 hours long, not 24.
  it should "keep a spring-forward day one calendar day, not 24 hours" in {
    val interval = firstSubInterval(new DateTime(2014, 3, 30, 12, 0, 0, Amsterdam), MultiDuration(1, TimeUnit.Day))
    interval.getStart should be(new DateTime(2014, 3, 30, 0, 0, 0, Amsterdam))
    interval.getEnd should be(new DateTime(2014, 3, 31, 0, 0, 0, Amsterdam))
    interval.toDuration.getStandardHours should be(23)
  }

  // And back at 03:00, making this one 25.
  it should "keep a fall-back day one calendar day, not 24 hours" in {
    val interval = firstSubInterval(new DateTime(2014, 10, 26, 12, 0, 0, Amsterdam), MultiDuration(1, TimeUnit.Day))
    interval.getStart should be(new DateTime(2014, 10, 26, 0, 0, 0, Amsterdam))
    interval.getEnd should be(new DateTime(2014, 10, 27, 0, 0, 0, Amsterdam))
    interval.toDuration.getStandardHours should be(25)
  }

  "Week alignment" should "start on Monday at local midnight, east of UTC" in {
    val interval = firstSubInterval(new DateTime(2014, 5, 8, 16, 26, 0, Amsterdam), MultiDuration(1, TimeUnit.Week))
    interval.getStart should be(new DateTime(2014, 5, 5, 0, 0, 0, Amsterdam))
    interval.getEnd should be(new DateTime(2014, 5, 12, 0, 0, 0, Amsterdam))
  }

  it should "start on Monday at local midnight, west of UTC" in {
    val interval = firstSubInterval(new DateTime(2014, 5, 8, 16, 26, 0, NewYork), MultiDuration(1, TimeUnit.Week))
    interval.getStart should be(new DateTime(2014, 5, 5, 0, 0, 0, NewYork))
    interval.getEnd should be(new DateTime(2014, 5, 12, 0, 0, 0, NewYork))
  }

  it should "align a multi-week bucket to a Monday" in {
    val interval = firstSubInterval(new DateTime(2014, 5, 8, 16, 26, 0, Amsterdam), MultiDuration(2, TimeUnit.Week))
    interval.getStart should be(new DateTime(2014, 4, 28, 0, 0, 0, Amsterdam))
    interval.getEnd should be(new DateTime(2014, 5, 12, 0, 0, 0, Amsterdam))
  }

  it should "span a DST transition inside the week" in {
    val interval = firstSubInterval(new DateTime(2014, 3, 30, 12, 0, 0, Amsterdam), MultiDuration(1, TimeUnit.Week))
    interval.getStart should be(new DateTime(2014, 3, 24, 0, 0, 0, Amsterdam))
    interval.getEnd should be(new DateTime(2014, 3, 31, 0, 0, 0, Amsterdam))
    interval.toDuration.getStandardHours should be((7 * 24) - 1)
  }

  // Deliberately pinned, because it is the one inconsistency here: sub-day buckets are aligned on the epoch rather
  // than on local midnight, so east of UTC a 6-hour bucket starts at 02:00, 08:00, 14:00, 20:00 local -- not at
  // midnight. Day and Week above do align locally. A port that "fixed" this silently would change every grouped
  // result for non-UTC consumers.
  "Sub-day alignment" should "bucket on the epoch rather than on local midnight" in {
    val interval = firstSubInterval(new DateTime(2014, 5, 8, 12, 0, 0, Amsterdam), MultiDuration(6, TimeUnit.Hour))
    interval.getStart should be(new DateTime(2014, 5, 8, 8, 0, 0, Amsterdam))
    interval.getEnd should be(new DateTime(2014, 5, 8, 14, 0, 0, Amsterdam))
  }

  it should "bucket minutes on the epoch, west of UTC" in {
    val interval = firstSubInterval(new DateTime(2014, 5, 8, 16, 26, 12, NewYork), MultiDuration(15, TimeUnit.Minute))
    interval.getStart should be(new DateTime(2014, 5, 8, 16, 15, 0, NewYork))
    interval.getEnd should be(new DateTime(2014, 5, 8, 16, 30, 0, NewYork))
  }

}
