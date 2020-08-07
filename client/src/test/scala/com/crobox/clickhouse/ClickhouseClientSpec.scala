package com.crobox.clickhouse

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{Assertion, BeforeAndAfterAll}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Random

abstract class ClickhouseClientSpec(val config: Config = ConfigFactory.load())
    extends TestKit(ActorSystem("clickhouseClientTestSystem", config.getConfig("crobox.clickhouse.client")))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  implicit val materializer: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext       = system.dispatcher

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(1.seconds, 50.millis)

  override protected def afterAll(): Unit =
    try super.afterAll()
    finally Await.result(system.terminate(), 10.seconds)

  def randomUUID: UUID =
    UUID.randomUUID

  def randomString: String =
    Random.alphanumeric.take(10).mkString

  def randomInt: Int =
    Random.nextInt(100000)

  // Returns the Clickhouse Version. DEFAUlt VALUE *must* equal the one set in .travis.yml AND docker-compose.xml
  lazy val ClickHouseVersion: String =
    Option(System.getenv("CLICKHOUSE_VERSION")).map(_.trim).filter(_.nonEmpty).getOrElse("20.3.12.112")
  lazy val ClickHouseMayorVersion: Int = ClickHouseVersion.substring(0, ClickHouseVersion.indexOf('.')).toInt

  def assumeMinimalClickhouseVersion(version: Int): Assertion =
    assume(ClickHouseMayorVersion >= version, s"ClickhouseVersion: ${} >= $version does NOT hold")

  def mustMatchClickHouseVersion(version: Int, testFun: => Any): Any =
    if (ClickHouseMayorVersion >= version) {
      // continue with test
      testFun
    } else {
      // abort test
      cancel()
    }

  private def clean(value: String) = value.replaceAll("[\\s\\n]", " ").replaceAll(" +", " ").trim

  def matchSQL(expected: String): Matcher[String] = new Matcher[String] {

    def apply(left: String): MatchResult =
      MatchResult(clean(left) == clean(expected),
                  s"SQL messages don't match. Input: ${clean(left)}",
                  "SQL messages are equal")
  }
}
