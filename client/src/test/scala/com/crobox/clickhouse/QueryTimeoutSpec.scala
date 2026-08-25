package com.crobox.clickhouse

import com.crobox.clickhouse.internal.QuerySettings
import com.crobox.clickhouse.internal.QuerySettings.ReadQueries
import com.typesafe.config.ConfigValueFactory

import scala.concurrent.Future
import scala.concurrent.duration._

class QueryTimeoutSpec extends ClickhouseClientAsyncSpec {

  private val client = new ClickhouseClient(Some(config))

  private val slowQuery = "SELECT count() FROM numbers(100000000000)"

  it should "fail a query that runs past its timeout" in {
    val started = System.nanoTime()
    client
      .query(slowQuery)(QuerySettings(ReadQueries, timeout = Some(1.second), retries = Some(0)))
      .failed
      .map { failure =>
        val elapsed = (System.nanoTime() - started).nanos
        withClue(s"failed with $failure after $elapsed: ") {
          failure shouldBe a[ClickhouseExecutionException]
          elapsed should be < 20.seconds
        }
      }
  }

  it should "leave a query that finishes inside its timeout alone" in
    client
      .query("SELECT 1")(QuerySettings(ReadQueries, timeout = Some(30.seconds)))
      .map(_.trim shouldBe "1")

  it should "not retry a timeout, since the deadline covers the whole call" in
    Future.successful(QueryTimeoutException(1.second, "SELECT 1").retryable shouldBe false)

  it should "take the timeout from config when the caller sets none" in {
    val timed = config.withValue(
      "crobox.clickhouse.client.settings.timeout",
      ConfigValueFactory.fromAnyRef("2 seconds")
    )
    val clientConfig = timed.getConfig("crobox.clickhouse.client")
    Future.successful(QuerySettings(ReadQueries).withFallback(clientConfig).timeout shouldBe Some(2.seconds))
  }

  it should "let the caller override the configured timeout" in {
    val timed = config.withValue(
      "crobox.clickhouse.client.settings.timeout",
      ConfigValueFactory.fromAnyRef("2 seconds")
    )
    val clientConfig = timed.getConfig("crobox.clickhouse.client")
    Future.successful(
      QuerySettings(ReadQueries, timeout = Some(5.seconds)).withFallback(clientConfig).timeout shouldBe Some(5.seconds)
    )
  }

  it should "stay unbounded when nothing configures a timeout" in
    Future.successful(
      QuerySettings(ReadQueries).withFallback(config.getConfig("crobox.clickhouse.client")).timeout shouldBe None
    )

  // The half max_execution_time cannot cover: a host that accepts the connection and then says nothing. Retries are
  // left on to show the deadline bounds the whole call rather than each attempt.
  it should "free the caller when the server never answers, without multiplying by the retries" in {
    val listener  = new java.net.ServerSocket(0)
    val accepting = new Thread(() => while (!listener.isClosed) scala.util.Try(listener.accept()))
    accepting.setDaemon(true)
    accepting.start()

    val stalled = new ClickhouseClient(
      Some(
        config
          .withValue("crobox.clickhouse.client.connection.port", ConfigValueFactory.fromAnyRef(listener.getLocalPort))
          .withValue("crobox.clickhouse.client.connection.host", ConfigValueFactory.fromAnyRef("localhost"))
      )
    )

    val started = System.nanoTime()
    stalled
      .query("SELECT 1")(QuerySettings(ReadQueries, timeout = Some(1.second), retries = Some(3)))
      .failed
      .map { failure =>
        val elapsed = (System.nanoTime() - started).nanos
        listener.close()
        withClue(s"failed with $failure after $elapsed: ") {
          failure shouldBe a[QueryTimeoutException]
          elapsed should be < 10.seconds
        }
      }
  }
}
