package com.crobox.clickhouse.partitioning

import java.util.Date

import java.time.{ZoneId, ZonedDateTime}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class PartitionDateFormatterTest extends AnyFlatSpecLike with Matchers {

  val expectedResult = "2017-12-31"
  val inputTimestamp = 1514709779000L

  it should "parse timestamp to partition date" in {
    PartitionDateFormatter.dateFormat(inputTimestamp) should be(expectedResult)
  }
  it should "parse a zoned date time to partition date" in {
    val zoned = ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(inputTimestamp), ZoneId.systemDefault())
    PartitionDateFormatter.dateFormat(zoned) should be(expectedResult)
  }

  it should "use the value's own zone rather than the system default" in {
    val instant = java.time.Instant.ofEpochMilli(inputTimestamp)
    PartitionDateFormatter.dateFormat(instant.atZone(ZoneId.of("UTC"))) should be("2017-12-31")
    // 08:42 UTC, so a zone ten hours behind is still on the previous day.
    PartitionDateFormatter.dateFormat(instant.atZone(ZoneId.of("Pacific/Honolulu"))) should be("2017-12-30")
  }
  it should "parse java date to partition date" in {
    PartitionDateFormatter.dateFormat(new Date(inputTimestamp)) should be(expectedResult)
  }
}
