package com.crobox.clickhouse.internal

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

/**
 * Pins the numeric result, which is the point: this expression was previously written out at four sites, and nothing
 * asserted on the value it produced. That let a truncate-to-second bug live for years, and let a thousandfold inflation
 * pass local tests when it was corrected.
 */
class ConfigDurationTest extends AnyFlatSpec with Matchers {

  private val config = ConfigFactory.parseString(
    """
      |whole = 10 seconds
      |sub = 500 millis
      |tiny = 1 millis
      |minutes = 2 minutes
      |""".stripMargin
  )

  it should "read a whole-second duration as itself" in {
    ConfigDuration(config, "whole") shouldBe 10.seconds
    ConfigDuration(config, "minutes") shouldBe 2.minutes
  }

  it should "preserve sub-second durations" in {
    // `getSeconds.seconds` floored these to zero; `toMillis.seconds` inflated them to 500 seconds.
    ConfigDuration(config, "sub") shouldBe 500.millis
    ConfigDuration(config, "tiny") shouldBe 1.milli
  }

  it should "not inflate whole seconds by a factor of a thousand" in {
    ConfigDuration(config, "whole") should be < 1.minute
  }

  it should "fall back only when the path is absent" in {
    ConfigDuration.orElse(config, "absent", 5.seconds) shouldBe 5.seconds
    ConfigDuration.orElse(config, "sub", 5.seconds) shouldBe 500.millis
  }
}
