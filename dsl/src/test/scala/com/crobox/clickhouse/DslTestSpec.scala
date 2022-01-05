package com.crobox.clickhouse

import com.crobox.clickhouse.dsl.language.ClickhouseTokenizerModule
import com.crobox.clickhouse.testkit.ClickhouseSpec
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

trait DslTestSpec
    extends AnyFlatSpec //Like
    with BeforeAndAfterAll
    with ScalaFutures
    with Matchers
    with ClickhouseSpec
    with TestSchema
    with ClickhouseSQLSupport
    with ClickhouseTokenizerModule {

  override val config: Config = ConfigFactory.load()
}
