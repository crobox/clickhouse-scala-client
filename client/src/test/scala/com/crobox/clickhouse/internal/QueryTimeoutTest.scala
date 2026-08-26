package com.crobox.clickhouse.internal

import com.crobox.clickhouse.internal.QuerySettings.AllQueries
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class QueryTimeoutTest extends AnyFlatSpecLike with Matchers {

  it should "send the timeout as max_execution_time in seconds" in {
    QuerySettings(AllQueries, timeout = Some(30.seconds)).asQueryParams.get("max_execution_time") shouldBe Some("30")
  }

  it should "send a sub-second timeout as a fraction" in {
    QuerySettings(AllQueries, timeout = Some(500.millis)).asQueryParams.get("max_execution_time") shouldBe Some("0.5")
  }

  it should "send nothing when no timeout is set" in {
    QuerySettings(AllQueries).asQueryParams.get("max_execution_time") shouldBe None
  }
}
