package com.crobox.clickhouse.internal

import org.apache.pekko
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.{ActorSystem, Terminated}
import org.apache.pekko.util.ByteString
import org.apache.pekko.http.scaladsl.{Http, HttpsConnectionContext}
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.settings.{ClientConnectionSettings, ConnectionPoolSettings}
import org.apache.pekko.stream._
import org.apache.pekko.stream.scaladsl.{Framing, Keep, Sink, Source, SourceQueueWithComplete}
import com.crobox.clickhouse.balancing.HostBalancer
import com.crobox.clickhouse.internal.progress.QueryProgress._
import com.crobox.clickhouse.internal.progress.{QueryProgress, StreamingProgressClickhouseTransport}
import com.crobox.clickhouse.internal.ClickhouseResponseParser.StreamedExceptionMarker
import com.crobox.clickhouse.{
  ClickhouseChunkedException,
  ClickhouseException,
  ClickhouseExecutionException,
  QueryTimeoutException,
  TooManyQueriesException
}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Random, Success}

private[clickhouse] trait ClickHouseExecutor extends LazyLogging {
  this: ClickhouseResponseParser with ClickhouseQueryBuilder =>

  protected implicit val system: ActorSystem
  protected implicit val executionContext: ExecutionContext
  protected val hostBalancer: HostBalancer
  protected val config: Config
  protected val customConnectionContext: Option[HttpsConnectionContext]

  lazy val (progressQueue, progressSource) = {
    val builtSource = QueryProgress.queryProgressStream.run()
    builtSource._2.runWith(Sink.ignore) // ensure we have one sink draining the progress
    builtSource
  }

  lazy val superPoolSettings: ConnectionPoolSettings = ConnectionPoolSettings(system)
    .withConnectionSettings(
      ClientConnectionSettings(system).withTransport(new StreamingProgressClickhouseTransport(progressQueue))
    )
  private lazy val http              = Http()
  private lazy val connectionContext = customConnectionContext.getOrElse(http.defaultClientHttpsContext)
  private lazy val pool              =
    http.superPool[Promise[HttpResponse]](connectionContext = connectionContext, settings = superPoolSettings)
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

  def executeRequest(
      query: String,
      settings: QuerySettings,
      entity: Option[RequestEntity] = None,
      progressQueue: Option[SourceQueueWithComplete[QueryProgress]] = None
  ): Future[String] = {
    val internalQueryIdentifier = queryIdentifier
    val totalRetries            = settings.retries.getOrElse(queryRetries)

    // Subscribed once per query and cancelled when the query settles. This used to happen inside
    // executeRequestInternal via a bare runForeach, which materialised a BroadcastHub consumer that nothing ever
    // cancelled -- one per request plus one per retry, each retained for the lifetime of the hub.
    val progressSubscription = progressQueue.map(subscribeToProgress(internalQueryIdentifier, _))

    val attempts = executeWithRetries(totalRetries, totalRetries, progressQueue, settings) { () =>
      executeRequestInternal(hostBalancer.nextHost, query, internalQueryIdentifier, settings, entity, progressQueue)
    }

    withTimeout(attempts, query, settings).andThen { case _ =>
      progressSubscription.foreach(_.shutdown())
      progressQueue.foreach(_.complete())
    }
  }

  /**
   * Bounds the whole call rather than one attempt, so the caller's wall clock is what the timeout describes -- a 30
   * second timeout with three retries would otherwise take 120 seconds to fail.
   *
   * The deadline is set slightly past `max_execution_time` so the server's own TIMEOUT_EXCEEDED normally lands first,
   * which reports the elapsed time; this only fires when the server never answers at all. Losing the race does not
   * cancel the request, but the response is still consumed downstream, so the connection is returned to the pool.
   */
  private def withTimeout(result: Future[String], query: String, settings: QuerySettings): Future[String] =
    settings.timeout match {
      case None          => result
      case Some(timeout) =>
        val deadline = pekko.pattern.after(timeout + ClickHouseExecutor.TimeoutGrace, system.scheduler)(
          Future.failed(QueryTimeoutException(timeout, query))
        )
        Future.firstCompletedOf(Seq(result, deadline))
    }

  private def subscribeToProgress(
      internalQueryIdentifier: String,
      target: SourceQueueWithComplete[QueryProgress]
  ): UniqueKillSwitch =
    progressSource
      .collect { case progress if progress.identifier == internalQueryIdentifier => progress.progress }
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.foreach(progress => target.offer(progress)))(Keep.left)
      .run()

  protected def queryIdentifier: String =
    Random.alphanumeric.take(20).mkString("")

  def executeRequestWithProgress(
      query: String,
      settings: QuerySettings,
      entity: Option[RequestEntity] = None
  ): Source[QueryProgress, Future[String]] =
    Source
      .queue[QueryProgress](10, OverflowStrategy.dropHead)
      .mapMaterializedValue(queue => executeRequest(query, settings, entity, Some(queue)))

  /**
   * Progress events and the result body in one stream, so a large result is not held whole.
   *
   * `executeRequestWithProgress` returns the body as its materialised value, which means draining the entity into a
   * single String -- a million-row result is one 7MB allocation. Here the body is framed and emitted as
   * [[QueryProgress.QueryResultPart]] events instead.
   *
   * No retries: a retry would re-run the request after the consumer had already seen part of a result.
   */
  def executeRequestStreaming(
      query: String,
      settings: QuerySettings,
      maximumFrameLength: Int
  ): Source[QueryProgress, NotUsed] = {
    val identifier = queryIdentifier

    val progress = progressSource.collect {
      case reported if reported.identifier == identifier => reported.progress
    }

    val body = Source
      .future(hostBalancer.nextHost.flatMap { host =>
        val request =
          toRequest(host, query, Some(identifier), settings.copy(progressHeaders = Some(true)), None)(config)
        singleRequest(request, progressEnabled = true)
      })
      // decodeResponse, because the request advertises gzip and deflate: without it a compressed response would be
      // framed as though the raw bytes were text.
      .map(decodeResponse)
      .flatMapConcat {
        case response @ HttpResponse(StatusCodes.OK, _, entity, _) =>
          entity.withoutSizeLimit().dataBytes.map(bytes => (response.status, bytes))
        case response =>
          // Non-OK: stream the body as the failure rather than as results.
          response.entity
            .withoutSizeLimit()
            .dataBytes
            .fold(ByteString.empty)(_ ++ _)
            .flatMapConcat(body =>
              Source.failed(
                ClickhouseException(
                  s"Server returned code ${response.status}; $body",
                  query,
                  statusCode = response.status
                )
              )
            )
      }
      .map(_._2)
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength, allowTruncation = true))
      .map { line =>
        val text = line.utf8String
        // ClickHouse answers 200 and starts streaming, then appends an error if the query fails partway through, so
        // the status line cannot report it. processClickhouseResponse checks the buffered body for the same marker;
        // streaming has to check each line, or a failure arrives as ordinary results followed by QueryFinished.
        StreamedExceptionMarker.findFirstMatchIn(text).foreach { marker =>
          throw ClickhouseException(
            s"Query failed after the response had started streaming; ClickHouse error code ${marker.group(1)}",
            query,
            ClickhouseChunkedException(text),
            StatusCodes.OK
          )
        }
        QueryResultPart(text)
      }
      .concat(Source.single(QueryFinished))

    // eagerComplete, because the progress hub is a BroadcastHub that never completes on its own -- the body is what
    // decides when this stream is done.
    val merged = progress.merge(body, eagerComplete = true)

    // idleTimeout rather than the deadline executeRequest applies. A stream's total duration is legitimately
    // unbounded -- a large result can take longer than any per-query timeout and still be healthy -- but a gap
    // between elements means the server has stopped answering, which is what the timeout is there to catch. The
    // server-side max_execution_time still bounds the query itself, since it travels as a query parameter.
    settings.timeout
      .map(timeout => merged.idleTimeout(timeout + ClickHouseExecutor.TimeoutGrace))
      .getOrElse(merged)
  }

  def shutdown(): Future[Terminated] = {
    queue.complete()
    queue
      .watchCompletion()
      .flatMap(_ => completion)
      .flatMap(_ => http.shutdownAllConnectionPools())
      .flatMap(_ => system.terminate())
  }

  protected def singleRequest(request: HttpRequest, progressEnabled: Boolean): Future[HttpResponse] =
    if (progressEnabled) {
      val promise = Promise[HttpResponse]()

      queue.offer(request -> promise).flatMap {
        case QueueOfferResult.Enqueued    => promise.future
        case QueueOfferResult.Dropped     => Future.failed(TooManyQueriesException())
        case QueueOfferResult.QueueClosed => Future.failed(new RuntimeException(s"Queue is closed"))
        case QueueOfferResult.Failure(e)  => Future.failed(e)
      }
    } else {
      http.singleRequest(request, connectionContext = connectionContext)
    }

  protected def executeRequestInternal(
      host: Future[Uri],
      query: String,
      queryIdentifier: String,
      settings: QuerySettings,
      entity: Option[RequestEntity] = None,
      progressQueue: Option[SourceQueueWithComplete[QueryProgress]]
  ): Future[String] =
    host.flatMap { actualHost =>
      val request = toRequest(
        actualHost,
        query,
        Some(queryIdentifier),
        settings.copy(
          progressHeaders = settings.progressHeaders.orElse(Some(progressQueue.isDefined))
        ),
        entity
      )(config)
      processClickhouseResponse(singleRequest(request, progressQueue.isDefined), query, actualHost, progressQueue)
    }

  private def executeWithRetries(
      retries: Int,
      totalRetries: Int,
      progressQueue: Option[SourceQueueWithComplete[QueryProgress]],
      settings: QuerySettings
  )(
      request: () => Future[String]
  ): Future[String] = {
    // Against totalRetries, not the config value: settings.retries can override it per query, and reporting
    // "retry 1 of 3" for a query configured with one retry is misleading.
    val attempt = (totalRetries - retries) + 1
    request().recoverWith {
      case clickException: ClickhouseExecutionException if !clickException.retryable =>
        // TODO use more fine grained exceptions in the client and remove the match on `Exception`
        Future.failed(clickException)
      case e: StreamTcpException if retries > 0 =>
        progressQueue.foreach(_.offer(QueryRetry(e, attempt)))
        logger.warn(s"Stream exception, retries left: $retries", e)
        executeWithRetries(retries - 1, totalRetries, progressQueue, settings)(request)
      case e: RuntimeException
          if e.getMessage.contains("The http server closed the connection unexpectedly") && retries > 0 =>
        logger.warn(s"Unexpected connection closure, retries left: $retries", e)
        progressQueue.foreach(_.offer(QueryRetry(e, attempt)))
        executeWithRetries(retries - 1, totalRetries, progressQueue, settings)(request)
      case e: Exception if settings.idempotent.contains(true) && retries > 0 =>
        logger.warn(s"Query execution exception while executing idempotent query, retries left: $retries", e)
        progressQueue.foreach(_.offer(QueryRetry(e, attempt)))
        executeWithRetries(retries - 1, totalRetries, progressQueue, settings)(request)
    }
  }
}

object ClickHouseExecutor {

  /** Head start for the server's own timeout, whose error names the elapsed time where the client's cannot. */
  private[internal] val TimeoutGrace: FiniteDuration = 1.second
}
