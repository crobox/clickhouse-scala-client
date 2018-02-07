package com.crobox.clickhouse.internal

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer, OverflowStrategy, QueueOfferResult}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

private[clickhouse] trait ClickHouseExecutor extends LazyLogging {
  this: ClickhouseResponseParser with ClickhouseQueryBuilder =>

  protected implicit val system: ActorSystem
  private implicit lazy val materializer: Materializer = ActorMaterializer()

  override protected def config: Config

  private lazy val pool = Http().superPool[Promise[HttpResponse]]()
  protected lazy val bufferSize: Int =
    config.getInt("crobox.clickhouse.client.buffer-size")
  private lazy val queue = Source
    .queue[(HttpRequest, Promise[HttpResponse])](bufferSize, OverflowStrategy.dropNew)
    .via(pool)
    .toMat(Sink.foreach {
      case ((Success(resp), p)) => p.success(resp)
      case ((Failure(e), p))    => p.failure(e)
    })(Keep.left)
    .run

  def executeRequest(host: Future[Uri],
                     query: String,
                     readOnly: Boolean = true,
                     entity: Option[RequestEntity] = None): Future[String] =
    host.flatMap(actualHost => {
      val request = toRequest(actualHost, query, readOnly, entity)
      handleResponse(singleRequest(request), query, actualHost)
    })

  def singleRequest(request: HttpRequest): Future[HttpResponse] = {
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

}
