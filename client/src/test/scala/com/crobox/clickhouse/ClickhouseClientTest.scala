package com.crobox.clickhouse

import com.typesafe.config.ConfigFactory

/**
 * @author Sjoerd Mulder
 * @author Yegor Andreenko
 * @since 31-3-17
 */
class ClickhouseClientTest extends ClickhouseClientAsyncSpec {

  val client: ClickhouseClient = new ClickhouseClient(config)

  it should "select" in {
    client
      .query("select 1 + 2")
      .map { f =>
        f.trim.toInt should be(3)
      }
      .flatMap(
        _ =>
          client.query("select currentDatabase()").map { f =>
            f.trim should be("default")
        }
      )
  }

  it should "support compression" in {
    val client: ClickhouseClient = new ClickhouseClient(
      config.resolveWith(ConfigFactory.parseString("crobox.clickhouse.client.http-compression = true"))
    )
    client.query("select count(*) from system.tables").map { f =>
      f.trim.toInt > 10 should be(true)
    }
  }

  it should "decline execute SELECT query" in {
    client.execute("select 1 + 2").map(_ => fail()).recover {
      case _: IllegalArgumentException => succeed
    }
  }
}
