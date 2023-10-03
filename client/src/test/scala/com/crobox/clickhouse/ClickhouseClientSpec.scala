package com.crobox.clickhouse

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import org.scalactic.{Tolerance, TripleEqualsSupport}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Random

abstract class ClickhouseClientSpec(val config: Config = ConfigFactory.load())
    extends TestKit(ActorSystem("clickhouseClientTestSystem", config.getConfig("crobox.clickhouse.client")))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  implicit val ec: ExecutionContext = system.dispatcher

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

  implicit class PercentageDelta[T: Numeric](value: T) extends Tolerance {
    type Base = T

    private val numeric: Numeric[T] = implicitly[Numeric[T]]

    def ~%(percent: Int, base: Base = numeric.fromInt(5)): TripleEqualsSupport.Spread[T] = {
      import numeric._
      value +- numeric.plus(base, numeric.fromInt(((value.toDouble / 100D) * percent).toInt))
    }
  }
}
