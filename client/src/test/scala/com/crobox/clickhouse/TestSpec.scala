package com.crobox.clickhouse

import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpecLike, Matchers}

trait TestSpec extends FlatSpecLike with Matchers {
  val config = ConfigFactory.load()
}
