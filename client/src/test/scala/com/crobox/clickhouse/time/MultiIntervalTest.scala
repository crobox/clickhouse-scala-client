package com.crobox.clickhouse.time

import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpecLike, Matchers}

class MultiIntervalTest extends FlatSpecLike with Matchers with TableDrivenPropertyChecks {

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

  "Sub intervals" should "build sub intervals and start end for all time units" in {
    forAll(
      Table(
        ("Time Unit", "End interval function", "Expected intervals"),
        (MultiDuration(1, TimeUnit.Second),
          (time: DateTime) => time.plusSeconds(1),
          IndexedSeq(toDateTime(2014, 5, 8, 16, 26, 12, 0) to toDateTime(2014, 5, 8, 16, 26, 12, 999),
            toDateTime(2014, 5, 8, 16, 26, 13, 0) to toDateTime(2014, 5, 8, 16, 26, 13, 999))),
        (MultiDuration(1, TimeUnit.Minute),
          (time: DateTime) => time.plusMinutes(1),
          IndexedSeq(toDateTime(2014, 5, 8, 16, 26, 0, 0) to toDateTime(2014, 5, 8, 16, 26, 59, 999),
            toDateTime(2014, 5, 8, 16, 27, 0, 0) to toDateTime(2014, 5, 8, 16, 27, 59, 999))),
        (MultiDuration(1, TimeUnit.Hour),
          (time: DateTime) => time.plusHours(1),
          IndexedSeq(toDateTime(2014, 5, 8, 16, 0, 0, 0) to toDateTime(2014, 5, 8, 16, 59, 59, 999),
            toDateTime(2014, 5, 8, 17, 0, 0, 0) to toDateTime(2014, 5, 8, 17, 59, 59, 999))),
        (MultiDuration(6, TimeUnit.Hour),
          (time: DateTime) => time.plusHours(1),
          IndexedSeq(toDateTime(2014, 5, 8, 12, 0, 0, 0) to toDateTime(2014, 5, 8, 17, 59, 59, 999))),
        (MultiDuration(6, TimeUnit.Hour),
          (time: DateTime) => time.plusHours(6),
          IndexedSeq(toDateTime(2014, 5, 8, 12, 0, 0, 0) to toDateTime(2014, 5, 8, 17, 59, 59, 999),
            toDateTime(2014, 5, 8, 18, 0, 0, 0) to toDateTime(2014, 5, 8, 23, 59, 59, 999))),
        (MultiDuration(1, TimeUnit.Day),
          (time: DateTime) => time.plusDays(1),
          IndexedSeq(toDateTime(2014, 5, 8, 0, 0, 0, 0) to toDateTime(2014, 5, 8, 23, 59, 59, 999),
            toDateTime(2014, 5, 9, 0, 0, 0, 0) to toDateTime(2014, 5, 9, 23, 59, 59, 999))),
        (MultiDuration(1, TimeUnit.Week),
          (time: DateTime) => time.plusWeeks(1),
          IndexedSeq(toDateTime(2014, 5, 5, 0, 0, 0, 0) to toDateTime(2014, 5, 11, 23, 59, 59, 999),
            toDateTime(2014, 5, 12, 0, 0, 0, 0) to toDateTime(2014, 5, 18, 23, 59, 59, 999))),
        (MultiDuration(3, TimeUnit.Week),
          (time: DateTime) => time.plusWeeks(1),
          IndexedSeq(toDateTime(2014, 5, 5, 0, 0, 0, 0) to toDateTime(2014, 5, 25, 23, 59, 59, 999))),
        (MultiDuration(1, TimeUnit.Month),
          (time: DateTime) => time.plusMonths(1),
          IndexedSeq(toDateTime(2014, 5, 1, 0, 0, 0, 0) to toDateTime(2014, 5, 31, 23, 59, 59, 999),
            toDateTime(2014, 6, 1, 0, 0, 0, 0) to toDateTime(2014, 6, 30, 23, 59, 59, 999))),
        (MultiDuration(2, TimeUnit.Month),
          (time: DateTime) => time.plusMonths(1),
          IndexedSeq(toDateTime(2014, 4, 1, 0, 0, 0, 0) to toDateTime(2014, 5, 31, 23, 59, 59, 999),
            toDateTime(2014, 6, 1, 0, 0, 0, 0) to toDateTime(2014, 7, 31, 23, 59, 59, 999))),
      )
    ) { (duration, intervalEnd, intervals) =>
    {
      val interval = MultiInterval(dateTime, intervalEnd(dateTime), duration)
      interval.startOfInterval() should be(intervals.head.getStart)
      interval.subIntervals() should contain theSameElementsInOrderAs intervals
      interval.endOfInterval() should be(intervals.last.getEnd)
    }
    }
  }

  it should "build sub intervals and start end for all simple time units" in {
    forAll(
      Table(
        ("Time Unit", "End interval function", "Expected intervals"),
        (MultiDuration(TimeUnit.Quarter),
          (time: DateTime) => time.plusMonths(3),
          IndexedSeq(toDateTime(2014, 4, 1, 0, 0, 0, 0) to toDateTime(2014, 6, 30, 23, 59, 59, 999),
            toDateTime(2014, 7, 1, 0, 0, 0, 0) to toDateTime(2014, 9, 30, 23, 59, 59, 999))),
        (MultiDuration(TimeUnit.Quarter),
          (time: DateTime) => time.plusMonths(1),
          IndexedSeq(toDateTime(2014, 4, 1, 0, 0, 0, 0) to toDateTime(2014, 6, 30, 23, 59, 59, 999))),
        (MultiDuration(TimeUnit.Year),
          (time: DateTime) => time.plusMonths(3),
          IndexedSeq(toDateTime(2014, 1, 1, 0, 0, 0, 0) to toDateTime(2014, 12, 31, 23, 59, 59, 999))),
        (SimpleDuration(TimeUnit.Total),
          (time: DateTime) => time.plusMonths(3),
          IndexedSeq(dateTime to dateTime.plusMonths(3)))
      )
    ) { (duration, intervalEnd, intervals) =>
    {
      val interval = MultiInterval(dateTime, intervalEnd(dateTime), duration)
      interval.startOfInterval() should be(intervals.head.getStart)
      interval.subIntervals() should contain theSameElementsInOrderAs intervals
      interval.endOfInterval() should be(intervals.last.getEnd)
    }
    }
  }

  it should "build correctly full time interval" in {
    val start    = DateTime.now().withTimeAtStartOfDay()
    val end      = start.plusDays(1)
    val interval = MultiInterval(start, end, SimpleDuration(TimeUnit.Total))
    interval.startOfInterval() should be(start)
    interval.subIntervals() should contain theSameElementsInOrderAs IndexedSeq(start to end)
    interval.endOfInterval() should be(end)
  }

  it should "include sub interval for which start date is equal to expected interval end date" in {
    val startDate = toDateTime(2012, 1, 1, 1, 1, 5, 0)
    val endDate   = toDateTime(2012, 1, 1, 1, 1, 10, 0)
    val interval  = MultiInterval(startDate, endDate, MultiDuration(5, TimeUnit.Second))
    interval.startOfInterval() should be(startDate)
    interval.endOfInterval() should be(endDate.plusSeconds(4).withMillisOfSecond(999))
    interval
      .subIntervals() should contain theSameElementsInOrderAs ((startDate to startDate.plusMillis(4999)) :: (endDate to endDate
      .plusMillis(4999)) :: Nil)
  }

}