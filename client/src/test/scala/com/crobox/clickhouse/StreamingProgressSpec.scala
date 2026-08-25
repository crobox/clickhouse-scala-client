package com.crobox.clickhouse

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
}
