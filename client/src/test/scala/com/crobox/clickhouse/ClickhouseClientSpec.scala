package com.crobox.clickhouse

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.Random

class ClickhouseClientSpec(val config: Config = ConfigFactory.load())
    extends TestKit(ActorSystem("clickhouseClientTestSystem", config))
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor    = system.dispatcher

  override implicit def patienceConfig: PatienceConfig  = PatienceConfig(1.seconds, 50.millis)

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
