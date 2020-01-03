package com.crobox.clickhouse

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

trait TestSpec extends AnyFlatSpecLike with Matchers {
  val config = ConfigFactory.load()
}
