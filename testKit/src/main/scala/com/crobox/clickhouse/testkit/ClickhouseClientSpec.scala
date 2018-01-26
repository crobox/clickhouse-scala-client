package com.crobox.clickhouse.testkit

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class ClickhouseClientSpec
    extends TestKit(ActorSystem("clickhouseClientTestSystem"))
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll {
  val config = ConfigFactory.load()

  override protected def afterAll(): Unit = {
    super.afterAll()
    system.terminate()
  }
}
