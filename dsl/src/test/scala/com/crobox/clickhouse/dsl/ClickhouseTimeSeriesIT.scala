package com.crobox.clickhouse.dsl

import java.util.UUID

import com.crobox.clickhouse.TestSchemaClickhouseQuerySpec
import com.crobox.clickhouse.dsl.execution.QueryResult
import com.crobox.clickhouse.dsl.marshalling.ClickhouseJsonSupport._
import com.crobox.clickhouse.ClickhouseClientSpec
import com.crobox.clickhouse.time.{IntervalStart, MultiDuration, MultiInterval, TotalDuration, TimeUnit}
import org.joda.time.{DateTime, DateTimeZone, Days}
import org.scalactic.TripleEqualsSupport
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.Future
import scala.util.Random

class ClickhouseTimeSeriesIT
    extends ClickhouseClientSpec
    with TestSchemaClickhouseQuerySpec
    with ScalaFutures
    with TableDrivenPropertyChecks {

  case class Result(time: IntervalStart, shields: String)
  override implicit def patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(20, Millis)))

  object Result {
    implicit val format = jsonFormat2(Result.apply)
  }

  implicit val clickhouseClient                   = clickClient
  val startInterval                               = DateTime.parse("2019-03-01T").withTimeAtStartOfDay().withZone(DateTimeZone.UTC)
  val secondsId                                   = UUID.randomUUID()
  val dayId                                       = UUID.randomUUID()
  val minutesId                                   = UUID.randomUUID()
  private val numberOfGeneratedEntries: Int       = 60 * 60 * 5
  private val numberOfGeneratedEntriesForDay: Int = Days.daysBetween(startInterval, startInterval.plusYears(5)).getDays

  val secondAndMinuteEntries: Seq[Table1Entry] = (0 until numberOfGeneratedEntries).flatMap(diff => {
    Table1Entry(secondsId, startInterval.plusSeconds(diff)) ::
    Table1Entry(minutesId, startInterval.plusMinutes(diff)) :: Nil
  }) ++ (0 until numberOfGeneratedEntriesForDay).map(day => Table1Entry(dayId, startInterval.plusDays(day)))

  override val table1Entries: Seq[Table1Entry] = secondAndMinuteEntries
  val alias                                    = new TableColumn[Long]("time") {}

  val lastSecondEntryDate = startInterval.plusSeconds(numberOfGeneratedEntries)
  val lastMinuteIntervalDate = startInterval.plusMinutes(numberOfGeneratedEntries)
  val lastDayIntervalDate = startInterval.plusDays(numberOfGeneratedEntriesForDay)

  "Grouping on total" should "return full result" in {
    val modifiedStartInterval                = startInterval.minus(12416)
    val multiInterval                        = MultiInterval(modifiedStartInterval, lastSecondEntryDate, TotalDuration)
    val results: Future[QueryResult[Result]] = getEntries(multiInterval, secondsId)
    val rows                                 = results.futureValue.rows
    rows.size should be(1)
    rows.head.time should be(modifiedStartInterval)
    rows.head.shields.toInt should be(numberOfGeneratedEntries)
  }

  "Grouping on second" should "return every appropriate interval" in {
    forAll(Table("Second", 1, 2, 3, 5, 10, 15, 20, 30)) { duration =>
      val multiInterval                        = MultiInterval(startInterval, lastSecondEntryDate, MultiDuration(duration, TimeUnit.Second))
      val results: Future[QueryResult[Result]] = getEntries(multiInterval, secondsId)
      val expectedIntervalStarts               = multiInterval.subIntervals.map(_.getStart.withZone(DateTimeZone.UTC))
      val rows                                 = results.futureValue.rows
      validateFullRows(rows, duration)
      rows.map(_.time) should contain theSameElementsInOrderAs expectedIntervalStarts
    }

  }

  "Grouping on minutes" should "return appropriate intervals" in {
    forAll(Table("Minute", 1, 2, 3, 5, 10, 15, 20, 30, 90)) { duration =>
      val expectedEntriesPerMinutes = 60
      val multiInterval             = MultiInterval(startInterval, lastSecondEntryDate, MultiDuration(duration, TimeUnit.Minute))
      forAll(Table("Timezone", multiInterval, shiftedTz(multiInterval))) { multiInterval =>
        val results: Future[QueryResult[Result]] = getEntries(multiInterval, secondsId)
        val expectedIntervalStarts               = multiInterval.subIntervals.map(_.getStart.withZone(DateTimeZone.UTC))
        val rows                                 = results.futureValue.rows
        validateFullRows(rows, expectedEntriesPerMinutes * duration)
        rows.map(_.time) should contain theSameElementsInOrderAs expectedIntervalStarts
      }
    }
  }

  "Grouping on hours" should "properly group interval" in {
    forAll(Table("Hour", 1, 2, 3, 4, 6, 8, 12)) { duration =>
      val expectedEntriesPerHour = 60
      val multiInterval          = MultiInterval(startInterval, lastMinuteIntervalDate, MultiDuration(duration, TimeUnit.Hour))
      forAll(Table("Timezone", multiInterval, shiftedTz(multiInterval))) { multiInterval =>
        val results: Future[QueryResult[Result]] = getEntries(multiInterval, minutesId)
        val expectedIntervalStarts               = multiInterval.subIntervals.map(_.getStart.withZone(DateTimeZone.UTC))
        val rows                                 = results.futureValue.rows
        val expectedCountInFullInterval          = expectedEntriesPerHour * duration
        validateFullRows(rows, expectedCountInFullInterval)
        rows.map(_.time) should contain theSameElementsInOrderAs expectedIntervalStarts
      }
    }
  }

  "Grouping on days" should "properly group intervals" in {
    forAll(Table("Day", 1, 2, 6, 7, 12, 15)) { duration =>
      val expectedEntriesPerDay = 1440
      val multiInterval         = MultiInterval(startInterval, lastMinuteIntervalDate, MultiDuration(duration, TimeUnit.Day))
      forAll(Table("Timezone", multiInterval, shiftedTz(multiInterval))) { multiInterval =>
        val results: Future[QueryResult[Result]] = getEntries(multiInterval, minutesId)
        val expectedIntervalStarts               = multiInterval.subIntervals.map(_.getStart.withZone(DateTimeZone.UTC))
        val rows                                 = results.futureValue.rows
        val expectedCountInFullInterval          = expectedEntriesPerDay * duration
        validateFullRows(rows, expectedCountInFullInterval)
        rows.map(_.time) should contain theSameElementsInOrderAs expectedIntervalStarts
      }
    }
  }


  "Grouping on weeks" should "properly group intervals" in {
    forAll(Table("Week", 1, 2, 3, 4)) { duration =>
      val multiInterval          = MultiInterval(startInterval, lastDayIntervalDate, MultiDuration(duration, TimeUnit.Week))
      forAll(Table("Timezone", multiInterval, shiftedTz(multiInterval))) { multiInterval =>
        val results: Future[QueryResult[Result]] = getEntries(multiInterval, dayId)
        val expectedIntervalStarts               = multiInterval.subIntervals.map(_.getStart.withZone(DateTimeZone.UTC))
        val rows                                 = results.futureValue.rows
        val expectedCountInFullInterval          = 7 * duration
        validateFullRows(rows, expectedCountInFullInterval)
        rows.map(_.time) should contain theSameElementsInOrderAs expectedIntervalStarts
      }
    }
  }

  "Grouping on months" should "properly group interval" in {
    forAll(Table("Months", 1, 2, 3, 7)) { duration =>
      val multiInterval = MultiInterval(startInterval, lastDayIntervalDate, MultiDuration(duration, TimeUnit.Month))
      forAll(Table("Timezone", multiInterval, shiftedTz(multiInterval))) { multiInterval =>
        val results: Future[QueryResult[Result]] = getEntries(multiInterval, dayId)
        val expectedIntervalStarts = multiInterval.subIntervals.map(_.getStart.withZone(DateTimeZone.UTC))
        val rows = results.futureValue.rows
        val expectedCountInFullInterval = duration * 30 +- duration * 3
        validateFullRows(rows, expectedCountInFullInterval)
        rows.map(_.time) should contain theSameElementsInOrderAs expectedIntervalStarts
      }
    }
  }

  "Grouping on quarters" should "properly group intervals" in {
    forAll(Table("Quarters", 1, 2, 3, 4)) { duration =>
      val multiInterval = MultiInterval(startInterval, lastDayIntervalDate, MultiDuration(duration, TimeUnit.Quarter))

      forAll(Table("Timezone", multiInterval, shiftedTz(multiInterval))) { multiInterval =>
        val results: Future[QueryResult[Result]] = getEntries(multiInterval, dayId)
        val expectedIntervalStarts               = multiInterval.subIntervals.map(_.getStart.withZone(DateTimeZone.UTC))
        val rows                                 = results.futureValue.rows
        val expectedCountInFullInterval          = duration * 90 +- duration * 3
        validateFullRows(rows, expectedCountInFullInterval)
        rows.map(_.time) should contain theSameElementsInOrderAs expectedIntervalStarts
      }
    }
  }

  "Grouping on years" should "properly group intervals" in {
    forAll(Table("Years", 1, 2, 3, 4)) { duration =>
      val multiInterval = MultiInterval(startInterval, lastDayIntervalDate, MultiDuration(duration, TimeUnit.Year))
      forAll(Table("Timezone", multiInterval, shiftedTz(multiInterval))) { multiInterval =>
        val results: Future[QueryResult[Result]] = getEntries(multiInterval, dayId)
        val expectedIntervalStarts = multiInterval.subIntervals.map(_.getStart.withZone(DateTimeZone.UTC))
        val rows = results.futureValue.rows
        val expectedCountInFullInterval = duration * 365 +- duration * 4
        validateFullRows(rows, expectedCountInFullInterval)
        rows.map(_.time) should contain theSameElementsInOrderAs expectedIntervalStarts
      }
    }
  }

  private def getEntries(multiInterval: MultiInterval, entriesId: UUID) =
    chExecuter.execute[Result](
      select(count() as "shields", toUInt64(timeSeries(timestampColumn, multiInterval)) as alias)
        .from(OneTestTable)
        .groupBy(alias)
        .orderBy(alias)
        .where(shieldId isEq entriesId)
    )

  private def validateFullRows(rows: Seq[Result], expectedCountInFullInterval: TripleEqualsSupport.Spread[Int]): Unit =
    //drop first and last as they might not be full intervals
    rows.drop(1).dropRight(1).foreach(row => row.shields.toInt should be(expectedCountInFullInterval))

  private def validateFullRows(rows: Seq[Result], expectedCountInFullInterval: Int): Unit =
  //drop first and last as they might not be full intervals
    rows.drop(1).dropRight(1).foreach(row => row.shields.toInt should be(expectedCountInFullInterval))

  private def shiftedTz(intv: MultiInterval): MultiInterval =
    MultiInterval(
      intv.rawStart.withZone(DateTimeZone.forOffsetHours(Random.nextInt(24) - 12)),
      intv.rawEnd.withZone(DateTimeZone.forOffsetHours(Random.nextInt(24) - 12)),
      intv.duration
    )
}
