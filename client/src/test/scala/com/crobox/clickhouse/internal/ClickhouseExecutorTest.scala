package com.crobox.clickhouse.internal
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{HttpResponse, Uri}
import org.apache.pekko.stream.scaladsl.{Sink, SourceQueue}
import org.apache.pekko.stream.{Materializer, StreamTcpException}
import com.crobox.clickhouse.balancing.HostBalancer
import com.crobox.clickhouse.balancing.iterator.CircularIteratorSet
import com.crobox.clickhouse.internal.QuerySettings.{AllQueries, ReadQueries}
import com.crobox.clickhouse.internal.progress.QueryProgress.{QueryProgress, QueryRetry}
import com.crobox.clickhouse.{ClickhouseClientAsyncSpec, TooManyQueriesException}
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}

class ClickhouseExecutorTest extends ClickhouseClientAsyncSpec {
  private val balancingHosts                  = Seq(Uri("http://host1"), Uri("http://host2"), Uri("http://host3"), Uri("http://host4"))
  private val hosts                           = new CircularIteratorSet(balancingHosts)
  private lazy val self                       = this
  private var response: Uri => Future[String] = _
  private lazy val executor = {
    new ClickHouseExecutor with ClickhouseResponseParser with ClickhouseQueryBuilder {
      override protected implicit val system: ActorSystem                = self.system
      override protected implicit val executionContext: ExecutionContext = system.dispatcher
      override protected val config: Config                              = self.config.getConfig("crobox.clickhouse.client")
      override protected val hostBalancer: HostBalancer = new HostBalancer {
        override def nextHost: Future[Uri] = Future.successful(hosts.next())
      }

      override protected def processClickhouseResponse(
          responseFuture: Future[HttpResponse],
          query: String,
          host: Uri,
          progressQueue: Option[
            SourceQueue[
              QueryProgress
            ]
          ]
      )(
          implicit materializer: Materializer,
          executionContext: ExecutionContext
      ): Future[String] = response(host)
    }
  }

  it should "retry all requests with stream tcp connection" in {
    val exception = new StreamTcpException("")
    response = _ => Future.failed(exception)
    executor
      .executeRequestWithProgress("", QuerySettings(ReadQueries))
      .runWith(Sink.seq[QueryProgress])
      .map(progress => {
        progress should contain theSameElementsAs Seq(QueryRetry(exception, 1),
                                                      QueryRetry(exception, 2),
                                                      QueryRetry(exception, 3))
      })
  }

  it should "retry idempotent queries for all exceptions" in {
    val exception = new IllegalArgumentException("please retry me")
    response = _ => Future.failed(exception)
    executor
      .executeRequestWithProgress("", QuerySettings(AllQueries, idempotent = Some(true)))
      .runWith(Sink.seq[QueryProgress])
      .map(progress => {
        progress should contain theSameElementsAs Seq(QueryRetry(exception, 1),
                                                      QueryRetry(exception, 2),
                                                      QueryRetry(exception, 3))
      })
  }

  it should "not retry non idempotent queries for non connection exception" in {
    val exception = new IllegalArgumentException("no retry")
    response = _ => Future.failed(exception)
    executor
      .executeRequestWithProgress("", QuerySettings(AllQueries))
      .runWith(Sink.seq[QueryProgress])
      .map(progress => {
        progress should contain theSameElementsAs Seq()
      })
  }

  it should "execute retries on the next balancer host" in {
    val exception   = new IllegalArgumentException("retry")
    var servedHosts = Seq[Uri]()
    response = uri => {
      servedHosts = servedHosts :+ uri
      Future.failed(exception)
    }
    executor
      .executeRequestWithProgress("", QuerySettings(AllQueries, idempotent = Some(true)))
      .runWith(Sink.seq[QueryProgress])
      .map(progress => {
        progress should contain theSameElementsAs Seq(QueryRetry(exception, 1),
                                                      QueryRetry(exception, 2),
                                                      QueryRetry(exception, 3))
        servedHosts should contain theSameElementsAs balancingHosts
      })
  }

  it should "not retry non retryable exceptions" in {
    val exception = TooManyQueriesException()
    response = _ => Future.failed(exception)
    executor
      .executeRequestWithProgress("", QuerySettings(AllQueries))
      .runWith(Sink.seq[QueryProgress])
      .map(progress => {
        progress should contain theSameElementsAs Seq()
      })
  }
}
