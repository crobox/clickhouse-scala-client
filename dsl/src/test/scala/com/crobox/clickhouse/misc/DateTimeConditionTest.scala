package com.crobox.clickhouse.misc

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl.misc.DateConditions.dateTimeCondition
import com.crobox.clickhouse.dsl.{NativeColumn, SelectQuery}
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, LocalDate}
import org.scalatest.prop.TableDrivenPropertyChecks

class DateTimeConditionTest extends DslTestSpec with TableDrivenPropertyChecks {

  forAll(
    Table(
      ("startDate", "endDate", "expected"),
      ("2018-01-02T00:00:00Z",
       Some("2018-01-05T00:00:00Z"),
       "date >= toDate('2018-01-02') AND date < toDate('2018-01-05')"),
      ("2018-01-02T00:00:00Z", None, "date >= toDate('2018-01-02')"),
      ("2018-01-02T02:00:00+02:00",
       Some("2018-01-05T02:00:00+02:00"),
       "date >= toDate('2018-01-02') AND date < toDate('2018-01-05')"),
      ("2018-01-02T22:00:00-02:00",
       Some("2018-01-05T22:00:00-02:00"),
       "date >= toDate('2018-01-03') AND date < toDate('2018-01-06')"),
      ("2018-01-02T22:00:00-02:00",
       Some("2018-01-05T23:00:00-02:00"),
       "date >= toDate('2018-01-03') AND date <= toDate('2018-01-06') AND ts < 1515200400000"),
      ("2018-01-02T23:00:00-02:00",
       Some("2018-01-05T22:00:00-02:00"),
       "date >= toDate('2018-01-03') AND ts >= 1514941200000 AND date < toDate('2018-01-06')"),
      ("2018-01-02T23:00:00-02:00", None, "date >= toDate('2018-01-03') AND ts >= 1514941200000"),
      ("2018-01-02T01:00:00+02:00",
       Some("2018-01-05T01:00:00+02:00"),
       "date >= toDate('2018-01-01') AND ts >= 1514847600000 AND date <= toDate('2018-01-04') AND ts < 1515106800000"),
      ("2018-01-02T00:00:00-01:00",
       Some("2018-01-05T00:00:00-01:00"),
       "date >= toDate('2018-01-02') AND ts >= 1514854800000 AND date <= toDate('2018-01-05') AND ts < 1515114000000"),
      ("2018-01-02T23:00:00-02:00",
       Some("2018-01-05T23:00:00-02:00"),
       "date >= toDate('2018-01-03') AND ts >= 1514941200000 AND date <= toDate('2018-01-06') AND ts < 1515200400000"),
    )
  ) { (startDateTime, endDateTime, expected) =>
    it should s"resolve $startDateTime/$endDateTime to correct query" in {

      toSql(
        SelectQuery(
          Seq(
            dateTimeCondition(
              NativeColumn[LocalDate]("date"),
              NativeColumn[Long]("ts"),
              parseDateTime(startDateTime),
              endDateTime.map(parseDateTime)
            )
          )
        ).internalQuery,
        None
      ) shouldBe s"SELECT $expected"
    }
  }

  val ISO_8601 = ISODateTimeFormat.dateOptionalTimeParser().withZoneUTC();

  def parseDateTime(value: String): DateTime =
    ISO_8601.parseDateTime(value.replace(' ', 'T'))
}
