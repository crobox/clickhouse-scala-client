package com.crobox.clickhouse

import com.crobox.clickhouse.internal.QuerySettings
import com.typesafe.config.ConfigValueFactory

import scala.concurrent.duration._
import com.crobox.clickhouse.internal.QuerySettings.ReadQueries
import com.crobox.clickhouse.internal.progress.QueryProgress._
import org.apache.pekko.stream.scaladsl.Sink

class StreamingProgressSpec extends ClickhouseClientAsyncSpec {

  private val client = new ClickhouseClient(Some(config))

  it should "emit the result as parts rather than one buffered body" in
    client
      .queryWithProgressStreaming("SELECT number FROM numbers(5)")
      .runWith(Sink.seq)
      .map { events =>
        val parts = events.collect { case QueryResultPart(data) => data }.filter(_.nonEmpty)
        parts should contain theSameElementsInOrderAs Seq("0", "1", "2", "3", "4")
      }

  it should "finish the stream once the body is done, despite the progress hub never completing" in
    client
      .queryWithProgressStreaming("SELECT 1")
      .runWith(Sink.seq)
      .map(events => events.last shouldBe QueryFinished)

  it should "stream a large result without collecting it" in
    client
      .queryWithProgressStreaming("SELECT number FROM numbers(200000)")
      .collect { case QueryResultPart(data) if data.nonEmpty => 1 }
      .runWith(Sink.fold(0)(_ + _))
      .map(_ shouldBe 200000)

  it should "surface progress events alongside the result" in
    client
      .queryWithProgressStreaming("SELECT number FROM numbers(100000)")(
        com.crobox.clickhouse.internal.QuerySettings(
          com.crobox.clickhouse.internal.QuerySettings.ReadQueries,
          progressHeaders = Some(true)
        )
      )
      .runWith(Sink.seq)
      .map { events =>
        // The result is what matters; progress headers are best-effort and the server may finish too fast to send any.
        events.count { case QueryResultPart(d) if d.nonEmpty => true; case _ => false } shouldBe 100000
      }

  // ClickHouse answers 200 and starts streaming, then appends the error if the query fails partway through. Without
  // checking each line, that error arrives as ordinary QueryResultParts followed by QueryFinished, and the consumer
  // cannot tell it from success.
  //
  // Which path it takes is not stable across versions: 25.3 sends 200 with the error appended, 25.8 answers 500 with
  // the error as the whole body. Either must fail the stream and name the code, so the assertion covers both rather
  // than pinning one server's choice.
  it should "fail the stream on an error appended mid-body" in
    client
      .queryWithProgressStreaming("SELECT number, throwIf(number = 3, 'boom') FROM numbers(10)")(
        QuerySettings(ReadQueries, settings = Map("max_block_size" -> "1"))
      )
      .runWith(Sink.seq)
      .failed
      .map { failure =>
        failure shouldBe a[ClickhouseException]
        failure.getMessage should include("395")
      }

  it should "not report QueryFinished when the body carried an error" in
    client
      .queryWithProgressStreaming("SELECT number, throwIf(number = 3, 'boom') FROM numbers(10)")(
        QuerySettings(ReadQueries, settings = Map("max_block_size" -> "1"))
      )
      .recover { case _ => QueryRejected }
      .runWith(Sink.seq)
      .map(events => events should not contain QueryFinished)

  it should "fail the stream on a non-OK response rather than streaming the error as results" in
    client
      .queryWithProgressStreaming("SELECT this_column_does_not_exist FROM system.one")
      .runWith(Sink.seq)
      .failed
      .map(_ shouldBe a[ClickhouseException])

  // The timeout from #355 bounds a whole call, which would be wrong for a stream: a large result can legitimately run
  // longer than any per-query timeout. It becomes an idle timeout here, catching a server that stops answering.
  it should "free the caller when a stalled server stops sending mid-stream" in {
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
      .queryWithProgressStreaming("SELECT 1")(QuerySettings(ReadQueries, timeout = Some(1.second)))
      .runWith(Sink.seq)
      .failed
      .map { _ =>
        val elapsed = (System.nanoTime() - started).nanos
        listener.close()
        elapsed should be < 15.seconds
      }
  }

  it should "not cut short a result that takes longer than the timeout to stream" in
    client
      .queryWithProgressStreaming("SELECT number FROM numbers(300000)")(
        QuerySettings(ReadQueries, timeout = Some(1.second))
      )
      .collect { case QueryResultPart(d) if d.nonEmpty => 1 }
      .runWith(Sink.fold(0)(_ + _))
      .map(_ shouldBe 300000)
}
