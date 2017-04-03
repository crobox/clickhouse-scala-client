package com.crobox.clickhouse

import akka.actor.ActorSystem
import org.scalatest.{FlatSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * @author Sjoerd Mulder
  * @author Yegor Andreenko
  * @since 31-3-17
  */
class ClickhouseClientTest extends FlatSpecLike with Matchers {

  val timeout = 10.seconds
  implicit val system = ActorSystem()
  val client = new ClickhouseClient("localhost")
  import scala.concurrent.ExecutionContext.Implicits.global

  it should "select" in {
    Await.result(client.query("select 1 + 2").map { f =>
      f.trim.toInt should be(3)
    }, timeout)

    Await.result(client.query("select currentDatabase()").map { f =>
      f.trim should be("default")
    }, timeout)
  }

  it should "support compression" in {
    val compressClient = client.withHttpCompression()
    Await.result(compressClient.query("select count(*) from system.tables").map { f =>
      f.trim.toInt > 10 should be(true)
    }, timeout)
  }

  it should "decline execute SELECT query" in {
    intercept[IllegalArgumentException] {
      Await.result(client.execute("select 1 + 2"), timeout)
    }
  }



}
@sjoerd

