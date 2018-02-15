package com.crobox.clickhouse.testkit

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.util.Random

class ClickhouseClientSpec(config: Config = ConfigFactory.load())
    extends TestKit(ActorSystem("clickhouseClientTestSystem"))
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override protected def afterAll(): Unit = {
    super.afterAll()
    system.terminate()
  }

  def randomUUID: UUID =
    UUID.randomUUID

  def randomString: String =
    Random.alphanumeric.take(10).mkString

  def randomInt: Int =
    Random.nextInt(100000)

}
