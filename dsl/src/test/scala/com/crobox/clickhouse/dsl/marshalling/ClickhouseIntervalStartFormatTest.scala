package com.crobox.clickhouse.dsl.marshalling

import com.crobox.clickhouse.dsl.marshalling.ClickhouseJsonSupport.ClickhouseIntervalStartFormat
import org.joda.time.{DateTime, DateTimeZone}
import spray.json.{JsNumber, JsString}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ClickhouseIntervalStartFormatTest extends AnyFlatSpec with Matchers {

  val zone = DateTimeZone.forID("Europe/Bucharest")

  it should "read using month relative" in {
    ClickhouseIntervalStartFormat.read(
      JsString(s"${ClickhouseIntervalStartFormat.RelativeMonthsSinceUnixStart + 3}_$zone")
    ) should be(new DateTime("1970-04-01T00:00:00.000+02:00", DateTimeZone.UTC))
  }

  it should "read using 0 as JsString" in {
    ClickhouseIntervalStartFormat.read(JsString("0")) should be(
      new DateTime("1970-01-01T00:00:00.000+00:00", DateTimeZone.UTC)
    )
  }

  it should "read using 0 as JsNumber" in {
    ClickhouseIntervalStartFormat.read(JsNumber(0)) should be(
      new DateTime("1970-01-01T00:00:00.000+00:00", DateTimeZone.UTC)
    )
  }

  it should "read date only" in {
    ClickhouseIntervalStartFormat.read(JsString(s"1970-12-17_$zone")) should be(
      new DateTime("1970-12-17T00:00:00.000+02:00", DateTimeZone.UTC)
    )
  }

  it should "read timestamp" in {
    val date = DateTime.now(DateTimeZone.UTC)
    ClickhouseIntervalStartFormat.read(JsString(s"${date.getMillis}")) should be(date)
    ClickhouseIntervalStartFormat.read(JsNumber(date.getMillis)) should be(date)
  }

}
