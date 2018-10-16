package com.crobox.clickhouse.internal

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.{ClientConnectionSettings, ConnectionPoolSettings}
import akka.stream._
import akka.stream.scaladsl.{BroadcastHub, Keep, Sink, Source, SourceQueueWithComplete}
import com.crobox.clickhouse.balancing.HostBalancer
import com.crobox.clickhouse.internal.ClickHouseExecutor.QuerySettings.ReadOnlySetting
import com.crobox.clickhouse.internal.ClickHouseExecutor._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.parsing.json.JSON
import scala.util.{Failure, Random, Success, Try}

private[clickhouse] trait ClickHouseExecutor extends LazyLogging {
  this: ClickhouseResponseParser with ClickhouseQueryBuilder =>

  protected implicit val system: ActorSystem
  protected implicit lazy val materializer: Materializer = ActorMaterializer()
  protected implicit val executionContext: ExecutionContext
  protected val hostBalancer: HostBalancer
  protected def config: Config

  lazy val (progressQueue, progressSource) = {
    val builtSource = Source
      .queue[String](1000, OverflowStrategy.dropHead)
      .map[Option[ClickhouseQueryProgress]](queryAndProgress => {
        queryAndProgress.split("\n", 2).toList match {
          case queryId :: ProgressHeadersAsEventsStage.AcceptedMark :: Nil =>
            Some(ClickhouseQueryProgress(queryId, QueryAccepted))
          case queryId :: progressJson :: Nil =>
            Try {
              val parsedJson = JSON.parseFull(progressJson).map(_.asInstanceOf[Map[String, String]])
              if (parsedJson.isEmpty || parsedJson.get.size != 3) {
                throw new IllegalArgumentException(s"Cannot extract progress from $parsedJson")
              } else {
                val jsonMap = parsedJson.get
                ClickhouseQueryProgress(
                  queryId,
                  Progress(jsonMap("read_rows").toLong, jsonMap("read_bytes").toLong, jsonMap("total_rows").toLong)
                )
              }
            } match {
              case Success(value) => Some(value)
              case Failure(exception) =>
                logger.warn(s"Failed to parse json $progressJson", exception)
                None
            }
          case other @ _ =>
            logger.warn(s"Could not get progress from $other")
            None

        }
      })
      .collect {
        case Some(progress) => progress
      }
      .withAttributes(ActorAttributes.supervisionStrategy({
        case ex @ _ =>
          logger.warn("Detected failure in the query progress stream, resuming operation.", ex)
          Supervision.Resume
      }))
      .toMat(BroadcastHub.sink)(Keep.both)
      .run()
    builtSource._2.runWith(Sink.ignore) //ensure we have one sink draining the progress
    builtSource
  }

  lazy val superPoolSettings: ConnectionPoolSettings = ConnectionPoolSettings(system)
    .withConnectionSettings(
      ClientConnectionSettings(system)
        .withTransport(new StreamingProgressClickhouseTransport(progressQueue))
    )

  private lazy val pool = Http().superPool[Promise[HttpResponse]](settings = superPoolSettings)
  protected lazy val bufferSize: Int =
    config.getInt("crobox.clickhouse.client.buffer-size")

  private lazy val (queue, completion) = Source
    .queue[(HttpRequest, Promise[HttpResponse])](bufferSize, OverflowStrategy.dropNew)
    .via(pool)
    .toMat(Sink.foreach {
      case (Success(resp), p) => p.success(resp)
      case (Failure(e), p)    => p.failure(e)
    })(Keep.both)
    .run

  private val queryRetries: Int = config.getInt("crobox.clickhouse.client.retries")

  def executeRequest(query: String,
                     settings: QuerySettings,
                     entity: Option[RequestEntity] = None,
                     progressQueue: Option[SourceQueueWithComplete[QueryProgress]] = None): Future[String] = {
    val queryIdentifier = Random.alphanumeric.take(20).mkString("")
    executeWithRetries(queryRetries, progressQueue) { () =>
      executeRequestInternal(hostBalancer.nextHost, query, queryIdentifier, settings, entity, progressQueue)
    }.andThen {
      case _ => progressQueue.foreach(_.complete())
    }
  }

  def executeRequestWithProgress(query: String,
                                 settings: QuerySettings,
                                 entity: Option[RequestEntity] = None): Source[QueryProgress, Future[String]] =
    Source
      .queue[QueryProgress](10, OverflowStrategy.dropHead)
      .mapMaterializedValue(queue => {
        executeRequest(query, settings, entity, Some(queue))
      })

  def shutdown(): Future[Done] = {
    queue.complete()
    queue.watchCompletion().flatMap(_ => completion)
  }

  protected def singleRequest(request: HttpRequest): Future[HttpResponse] = {
    val promise = Promise[HttpResponse]

    queue.offer(request -> promise).flatMap {
      case QueueOfferResult.Enqueued =>
        promise.future
      case QueueOfferResult.Dropped =>
        Future.failed(new RuntimeException(s"Queue is full"))
      case QueueOfferResult.QueueClosed =>
        Future.failed(new RuntimeException(s"Queue is closed"))
      case QueueOfferResult.Failure(e) =>
        Future.failed(e)
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
                              settings.withFallback(config),
                              entity,
                              progressQueue.isDefined)
      processClickhouseResponse(singleRequest(request), query, actualHost, progressQueue)
    })
  }
  private def executeWithRetries(retries: Int, progressQueue: Option[SourceQueueWithComplete[QueryProgress]])(
      request: () => Future[String]
  ): Future[String] =
    request().recoverWith {
      // The http server closed the connection unexpectedly before delivering responses for 1 outstanding requests
      case e: StreamTcpException if retries > 0 =>
        progressQueue.foreach(_.offer(QueryRetry(e, (queryRetries - retries) + 1)))
        logger.warn(s"Stream exception, retries left: $retries", e)
        executeWithRetries(retries - 1, progressQueue)(request)
      case e: RuntimeException
          if e.getMessage.contains("The http server closed the connection unexpectedly") && retries > 0 =>
        logger.warn(s"Unexpected connection closure, retries left: $retries", e)
        progressQueue.foreach(_.offer(QueryRetry(e, (queryRetries - retries) + 1)))
        executeWithRetries(retries - 1, progressQueue)(request)
    }
}

object ClickHouseExecutor {
  val InternalQueryIdentifier = "X-Internal-Identifier"

  sealed trait QueryProgress
  case object QueryAccepted                                 extends QueryProgress
  case object QueryFinished                                 extends QueryProgress
  case object QueryRejected                                 extends QueryProgress
  case class QueryFailed(cause: Throwable)                  extends QueryProgress
  case class QueryRetry(cause: Throwable, retryNumber: Int) extends QueryProgress

  case class ClickhouseQueryProgress(identifier: String, progress: QueryProgress)
  case class Progress(rowsRead: Long, bytesRead: Long, totalRows: Long) extends QueryProgress

  case class QuerySettings(readOnly: ReadOnlySetting,
                           authentication: Option[(String, String)] = None,
                           progressHeaders: Option[Boolean] = None,
                           queryId: Option[String] = None,
                           profile: Option[String] = None,
                           httpCompression: Option[Boolean] = None) {

    def asQueryParams: Query =
      Query(
        Seq("readonly"         -> readOnly.value.toString) ++
        queryId.map("query_id" -> _) ++
        authentication.map(
          auth => "user" -> auth._1
        ) ++
        authentication.map(auth => "password" -> auth._2) ++
        profile.map("profile" -> _) ++
        progressHeaders.map(
          progress => "send_progress_in_http_headers" -> (if (progress) "1" else "0")
        ) ++
        httpCompression.map(compression => "enable_http_compression" -> (if (compression) "1" else "0")): _*
      )

    def withFallback(config: Config): QuerySettings =
      this.copy(
        authentication = authentication.orElse(Try {
          val authConfig = config.getConfig(path("authentication"))
          (authConfig.getString("user"), authConfig.getString("password"))
        }.toOption),
        profile = profile.orElse(Try { config.getString(path("profile")) }.toOption),
        httpCompression = httpCompression.orElse(Try { config.getBoolean(path("http-compression")) }.toOption)
      )

    private def path(setting: String) = s"crobox.clickhouse.client.settings.$setting"

  }

  object QuerySettings {
    sealed trait ReadOnlySetting {
      val value: Int
    }
    case object AllQueries extends ReadOnlySetting {
      override val value: Int = 0
    }
    case object ReadQueries extends ReadOnlySetting {
      override val value: Int = 1
    }
    case object ReadAndChangeQueries extends ReadOnlySetting {
      override val value: Int = 2
    }
  }
}
