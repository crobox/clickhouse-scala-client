package com.crobox.clickhouse.partitioning

import java.util.Date

import org.joda.time.DateTime
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class PartitionDateFormatterTest extends AnyFlatSpecLike with Matchers {

  val expectedResult = "2017-12-31"
  val inputTimestamp = 1514709779000L

  it should "parse timestamp to partition date" in {
    PartitionDateFormatter.dateFormat(inputTimestamp) should be(expectedResult)
  }
  it should "parse joda date time to partition date" in {
    PartitionDateFormatter.dateFormat(new DateTime(inputTimestamp)) should be(expectedResult)
  }
  it should "parse java date to partition date" in {
    PartitionDateFormatter.dateFormat(new Date(inputTimestamp)) should be(expectedResult)
  }
}
