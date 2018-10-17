package com.crobox.clickhouse

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.ExecutionContextExecutor

class ClickhouseClientSpec
    extends TestKit(ActorSystem("clickhouseClientTestSystem"))
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {
  val config: Config                           = ConfigFactory.load()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor    = system.dispatcher

  override protected def afterAll(): Unit = {
    super.afterAll()
    system.terminate()
  }
}
