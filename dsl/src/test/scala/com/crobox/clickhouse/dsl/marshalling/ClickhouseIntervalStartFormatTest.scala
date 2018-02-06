package com.crobox.clickhouse.dsl.marshalling

import com.crobox.clickhouse.dsl.marshalling.ClickhouseJsonSupport.ClickhouseIntervalStartFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{FlatSpec, Matchers}
import spray.json.{JsNumber, JsString}

class ClickhouseIntervalStartFormatTest extends FlatSpec with Matchers {

  val zone = DateTimeZone.forID("Europe/Bucharest")

  it should "read using month relative" in {
    ClickhouseIntervalStartFormat.read(
      JsString(s"${ClickhouseIntervalStartFormat.RelativeMonthsSinceUnixStart + 3}_$zone")
    ) should be(new DateTime("1970-04-01T00:00:00.000+02:00", DateTimeZone.UTC))
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
