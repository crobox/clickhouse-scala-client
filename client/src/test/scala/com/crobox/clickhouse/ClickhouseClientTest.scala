package com.crobox.clickhouse

import akka.stream.scaladsl.{Keep, Sink}
import com.crobox.clickhouse.internal.progress.QueryProgress.{Progress, QueryAccepted, QueryFinished, QueryProgress}
import com.typesafe.config.ConfigFactory

/**
 * @author Sjoerd Mulder
 * @author Leonard Wolters
 * @since 31-3-17
 */
class ClickhouseClientTest extends ClickhouseClientAsyncSpec {

  val client: ClickhouseClient = new ClickhouseClient(Some(config))

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
      Some(config.resolveWith(ConfigFactory.parseString("crobox.clickhouse.client.http-compression = true")))
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

  // flaky test
  ignore should "publish query progress messages" in {
    client
      .queryWithProgress("select 1 + 2")
      .runWith(Sink.seq[QueryProgress])
      .map(progress => progress should contain theSameElementsAs Seq(QueryAccepted, QueryFinished))
  }

  it should "materialize progress source with the query result" in {
    client
      .queryWithProgress("select 1 + 2")
      .toMat(Sink.ignore)(Keep.left)
      .run()
      .map(result => result.shouldBe("3\n"))
  }

  // This test is failing using new clickhouse server; apparently too fast?
  ignore should "send full progress messages" in {
    client
      .queryWithProgress("select sum(number) FROM (select number from system.numbers limit 100000000)")
      .runWith(Sink.seq[QueryProgress])
      .map(progress => {
        progress collect {
          case qp: Progress => qp
        } should not be empty
      })
  }

}
