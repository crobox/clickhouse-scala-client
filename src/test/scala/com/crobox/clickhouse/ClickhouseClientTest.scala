package com.crobox.clickhouse

import com.crobox.clickhouse.test.ClickhouseClientSpec
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * @author Sjoerd Mulder
  * @author Yegor Andreenko
  * @since 31-3-17
  */
class ClickhouseClientTest extends ClickhouseClientSpec {

  val timeout = 10.seconds

  val client: ClickhouseClient = new ClickhouseClient(config)

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
    val client: ClickhouseClient = new ClickhouseClient(
      config.resolveWith(ConfigFactory.parseString(
        "com.crobox.clickhouse.client.httpCompression = true")))
    Await.result(client.query("select count(*) from system.tables").map { f =>
      f.trim.toInt > 10 should be(true)
    }, timeout)
  }

  it should "decline execute SELECT query" in {
    intercept[IllegalArgumentException] {
      Await.result(client.execute("select 1 + 2"), timeout)
    }
  }
}
