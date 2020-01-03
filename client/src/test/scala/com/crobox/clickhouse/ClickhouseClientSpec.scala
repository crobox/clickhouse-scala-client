package com.crobox.clickhouse

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.concurrent.duration._
import scala.util.Random
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

abstract class ClickhouseClientSpec(val config: Config = ConfigFactory.load())
    extends TestKit(ActorSystem("clickhouseClientTestSystem", config.getConfig("crobox.clickhouse.client")))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  implicit val materializer: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  override implicit def patienceConfig: PatienceConfig  = PatienceConfig(1.seconds, 50.millis)

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally system.terminate()
  }

  def randomUUID: UUID =
    UUID.randomUUID

  def randomString: String =
    Random.alphanumeric.take(10).mkString

  def randomInt: Int =
    Random.nextInt(100000)
}
