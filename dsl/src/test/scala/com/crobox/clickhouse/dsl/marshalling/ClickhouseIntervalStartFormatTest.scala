package com.crobox.clickhouse.dsl.marshalling

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl.marshalling.ClickhouseJsonSupport.ClickhouseIntervalStartFormat
import com.crobox.clickhouse.time.IntervalStart
import java.time.{Instant, LocalDate, ZoneId, ZoneOffset, ZonedDateTime}
import spray.json.{JsNumber, JsString}

class ClickhouseIntervalStartFormatTest extends DslTestSpec {

  val zone = ZoneId.of("Europe/Bucharest")

  it should "read using month relative" in {
    ClickhouseIntervalStartFormat.read(
      JsString(s"${ClickhouseIntervalStartFormat.RelativeMonthsSinceUnixStart + 3}_$zone")
    ) should be(ZonedDateTime.parse("1970-04-01T00:00:00.000+02:00").withZoneSameInstant(ZoneOffset.UTC))
  }

  it should "read using 0 as JsString" in {
    ClickhouseIntervalStartFormat.read(JsString("0")) should be(
      ZonedDateTime.parse("1970-01-01T00:00:00.000+00:00").withZoneSameInstant(ZoneOffset.UTC)
    )
  }

  it should "read using 0 as JsNumber" in {
    ClickhouseIntervalStartFormat.read(JsNumber(0)) should be(
      ZonedDateTime.parse("1970-01-01T00:00:00.000+00:00").withZoneSameInstant(ZoneOffset.UTC)
    )
  }

  it should "read date only" in {
    ClickhouseIntervalStartFormat.read(JsString(s"1970-12-17_$zone")) should be(
      ZonedDateTime.parse("1970-12-17T00:00:00.000+02:00").withZoneSameInstant(ZoneOffset.UTC)
    )
  }

  it should "read timestamp" in {
    // Truncated because the wire format carries milliseconds while ZonedDateTime.now carries nanoseconds.
    val date = ZonedDateTime.now(ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
    ClickhouseIntervalStartFormat.read(JsString(s"${date.toInstant.toEpochMilli}")) should be(date)
    ClickhouseIntervalStartFormat.read(JsNumber(date.toInstant.toEpochMilli)) should be(date)
  }

  it should "convert to correct year" in {
    // JsString("1618876800000000").convertTo[IntervalStart] should be ("53270-03-01T00:00:00.000Z")
    JsString("1618876800000000").convertTo[IntervalStart].toInstant.toEpochMilli should be(1618876800000L)
  }

  it should "read date only for a timezone whose id contains an underscore" in {
    ClickhouseIntervalStartFormat.read(JsString("1970-12-17_America/New_York")) should be(
      ZonedDateTime.parse("1970-12-17T00:00:00.000-05:00").withZoneSameInstant(ZoneOffset.UTC)
    )
  }

  it should "read date only for a timezone with two underscores" in {
    ClickhouseIntervalStartFormat.read(JsString("1970-12-17_America/Argentina/Rio_Gallegos")) should be(
      ZonedDateTime.parse("1970-12-17T00:00:00.000-03:00").withZoneSameInstant(ZoneOffset.UTC)
    )
  }

  it should "read month relative for a timezone whose id contains an underscore" in {
    ClickhouseIntervalStartFormat.read(
      JsString(s"${ClickhouseIntervalStartFormat.RelativeMonthsSinceUnixStart + 3}_America/New_York")
    ) should be(ZonedDateTime.parse("1970-04-01T00:00:00.000-05:00").withZoneSameInstant(ZoneOffset.UTC))
  }

  it should "read a month before the unix epoch" in {
    ClickhouseIntervalStartFormat.read(
      JsString(s"${ClickhouseIntervalStartFormat.RelativeMonthsSinceUnixStart - 2}_$zone")
    ) should be(ZonedDateTime.parse("1969-11-01T00:00:00.000+02:00").withZoneSameInstant(ZoneOffset.UTC))
  }

  it should "read a quarter boundary as a date" in {
    ClickhouseIntervalStartFormat.read(JsString(s"1970-04-01_$zone")) should be(
      ZonedDateTime.parse("1970-04-01T00:00:00.000+02:00").withZoneSameInstant(ZoneOffset.UTC)
    )
  }

  it should "read a year boundary as a date" in {
    ClickhouseIntervalStartFormat.read(JsString(s"1971-01-01_$zone")) should be(
      ZonedDateTime.parse("1971-01-01T00:00:00.000+02:00").withZoneSameInstant(ZoneOffset.UTC)
    )
  }

  it should "read a ten-digit value as seconds" in {
    ClickhouseIntervalStartFormat.read(JsString("1618876800")) should be(
      Instant.ofEpochMilli(1618876800000L).atZone(ZoneOffset.UTC)
    )
  }

  it should "read a sixteen-digit value as microseconds" in {
    ClickhouseIntervalStartFormat.read(JsString("1618876800000000")).toInstant.toEpochMilli should be(1618876800000L)
  }

  it should "write millis" in {
    val date = Instant.ofEpochMilli(1618876800000L).atZone(ZoneOffset.UTC)
    ClickhouseIntervalStartFormat.write(date) should be(JsNumber(1618876800000L))
  }

  it should "refuse a value it cannot parse rather than returning an epoch" in {
    a[Exception] should be thrownBy ClickhouseIntervalStartFormat.read(JsString("not-a-date"))
  }
}
