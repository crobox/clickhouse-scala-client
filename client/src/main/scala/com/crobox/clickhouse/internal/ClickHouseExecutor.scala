package com.crobox.clickhouse.internal

import org.apache.pekko.actor.{ActorSystem, Terminated}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.settings.{ClientConnectionSettings, ConnectionPoolSettings}
import org.apache.pekko.stream._
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete}
import com.crobox.clickhouse.balancing.HostBalancer
import com.crobox.clickhouse.internal.progress.QueryProgress._
import com.crobox.clickhouse.internal.progress.{QueryProgress, StreamingProgressClickhouseTransport}
import com.crobox.clickhouse.{ClickhouseExecutionException, TooManyQueriesException}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Random, Success}

private[clickhouse] trait ClickHouseExecutor extends LazyLogging {
  this: ClickhouseResponseParser with ClickhouseQueryBuilder =>

  protected implicit val system: ActorSystem
  protected implicit val executionContext: ExecutionContext
  protected val hostBalancer: HostBalancer
  protected val config: Config

  lazy val (progressQueue, progressSource) = {
    val builtSource = QueryProgress.queryProgressStream.run()
    builtSource._2.runWith(Sink.ignore) //ensure we have one sink draining the progress
    builtSource
  }

  lazy val superPoolSettings: ConnectionPoolSettings = ConnectionPoolSettings(system)
    .withConnectionSettings(
      ClientConnectionSettings(system).withTransport(new StreamingProgressClickhouseTransport(progressQueue))
    )
  private lazy val http              = Http()
  private lazy val pool              = http.superPool[Promise[HttpResponse]](settings = superPoolSettings)
  private lazy val bufferSize: Int   = config.getInt("buffer-size")
  private lazy val queryRetries: Int = config.getInt("retries")

  private lazy val (queue, completion) = Source
    .queue[(HttpRequest, Promise[HttpResponse])](bufferSize, OverflowStrategy.backpressure)
    .via(pool)
    .toMat(Sink.foreach {
      case (Success(resp), p) => p.success(resp)
      case (Failure(e), p)    => p.failure(e)
    })(Keep.both)
    .run()

  def executeRequest(query: String,
                     settings: QuerySettings,
                     entity: Option[RequestEntity] = None,
                     progressQueue: Option[SourceQueueWithComplete[QueryProgress]] = None): Future[String] = {
    val internalQueryIdentifier = queryIdentifier
    executeWithRetries(settings.retries.getOrElse(queryRetries), progressQueue, settings) { () =>
      executeRequestInternal(hostBalancer.nextHost, query, internalQueryIdentifier, settings, entity, progressQueue)
    }.andThen {
      case _ => progressQueue.foreach(_.complete())
    }
  }

  protected def queryIdentifier: String =
    Random.alphanumeric.take(20).mkString("")

  def executeRequestWithProgress(query: String,
                                 settings: QuerySettings,
                                 entity: Option[RequestEntity] = None): Source[QueryProgress, Future[String]] =
    Source
      .queue[QueryProgress](10, OverflowStrategy.dropHead)
      .mapMaterializedValue(queue => executeRequest(query, settings, entity, Some(queue)))

  def shutdown(): Future[Terminated] = {
    queue.complete()
    queue
      .watchCompletion()
      .flatMap(_ => completion)
      .flatMap(_ => http.shutdownAllConnectionPools())
      .flatMap(_ => system.terminate())
  }

  protected def singleRequest(request: HttpRequest): Future[HttpResponse] = {
    val promise = Promise[HttpResponse]()

    queue.offer(request -> promise).flatMap {
      case QueueOfferResult.Enqueued    => promise.future
      case QueueOfferResult.Dropped     => Future.failed(TooManyQueriesException())
      case QueueOfferResult.QueueClosed => Future.failed(new RuntimeException(s"Queue is closed"))
      case QueueOfferResult.Failure(e)  => Future.failed(e)
    }
  }

  protected def executeRequestInternal(
      host: Future[Uri],
      query: String,
      queryIdentifier: String,
      settings: QuerySettings,
      entity: Option[RequestEntity] = None,
      progressQueue: Option[SourceQueueWithComplete[QueryProgress]]
  ): Future[String] = {
    progressQueue.foreach(definedProgressQueue => {
      progressSource.runForeach(
        progress => {
          if (progress.identifier == queryIdentifier) {
            definedProgressQueue.offer(progress.progress)
          }
        }
      )
    })
    host.flatMap(actualHost => {
      val request = toRequest(actualHost,
                              query,
                              Some(queryIdentifier),
                              settings.copy(
                                progressHeaders = settings.progressHeaders.orElse(Some(progressQueue.isDefined))
                              ),
                              entity)(config)
      processClickhouseResponse(singleRequest(request), query, actualHost, progressQueue)
    })
  }

  private def executeWithRetries(retries: Int,
                                 progressQueue: Option[SourceQueueWithComplete[QueryProgress]],
                                 settings: QuerySettings)(
      request: () => Future[String]
  ): Future[String] =
    request().recoverWith {
      case clickException: ClickhouseExecutionException if !clickException.retryable =>
        // TODO use more fine grained exceptions in the client and remove the match on `Exception`
        Future.failed(clickException)
      case e: StreamTcpException if retries > 0 =>
        progressQueue.foreach(_.offer(QueryRetry(e, (queryRetries - retries) + 1)))
        logger.warn(s"Stream exception, retries left: $retries", e)
        executeWithRetries(retries - 1, progressQueue, settings)(request)
      case e: RuntimeException
          if e.getMessage.contains("The http server closed the connection unexpectedly") && retries > 0 =>
        logger.warn(s"Unexpected connection closure, retries left: $retries", e)
        progressQueue.foreach(_.offer(QueryRetry(e, (queryRetries - retries) + 1)))
        executeWithRetries(retries - 1, progressQueue, settings)(request)
      case e: Exception if settings.idempotent.contains(true) && retries > 0 =>
        logger.warn(s"Query execution exception while executing idempotent query, retries left: $retries", e)
        progressQueue.foreach(_.offer(QueryRetry(e, (queryRetries - retries) + 1)))
        executeWithRetries(retries - 1, progressQueue, settings)(request)
    }
}

object ClickHouseExecutor {}
