package com.crobox.clickhouse.internal
import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpResponse, Uri}
import akka.stream.scaladsl.{Sink, SourceQueue}
import akka.stream.{Materializer, StreamTcpException}
import com.crobox.clickhouse.ClickhouseClientAsyncSpec
import com.crobox.clickhouse.balancing.HostBalancer
import com.crobox.clickhouse.internal.ClickHouseExecutor.{QueryProgress, QueryRetry, QuerySettings}
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}

class ClickhouseExecutorTest extends ClickhouseClientAsyncSpec {
  private val self                     = this
  private var response: Future[String] = Future.successful("")
  private val executor = new ClickHouseExecutor with ClickhouseResponseParser with ClickhouseQueryBuilder {
    override protected implicit val system: ActorSystem                = self.system
    override protected val hostBalancer: HostBalancer                  = HostBalancer(config)
    override protected def config: Config                              = self.config
    override protected implicit val executionContext: ExecutionContext = system.dispatcher
    override protected def processClickhouseResponse(
        responseFuture: Future[HttpResponse],
        query: String,
        host: Uri,
        progressQueue: Option[
          SourceQueue[
            ClickHouseExecutor.QueryProgress
          ]
        ]
    )(
        implicit materializer: Materializer
    ): Future[String] =
      response
  }

  it should "retry requests" in {
    val exception = new StreamTcpException("")
    response = Future.failed(exception)
    executor
      .executeRequestWithProgress("", QuerySettings())
      .runWith(Sink.seq[QueryProgress])
      .map(progress => {
        progress should contain theSameElementsAs Seq(QueryRetry(exception, 1),
                                                      QueryRetry(exception, 2),
                                                      QueryRetry(exception, 3))
      })
  }
}
