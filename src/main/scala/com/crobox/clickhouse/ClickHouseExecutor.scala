package com.crobox.clickhouse

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{HttpEncoding, HttpEncodingRange, HttpEncodings}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

private[clickhouse] trait ClickHouseExecutor extends LazyLogging {
  implicit val system: ActorSystem
  implicit val materializer: Materializer

  import system.dispatcher

  val bufferSize: Int
  private lazy val pool = Http().superPool[Promise[HttpResponse]]()
  private lazy val queue = Source.queue[(HttpRequest, Promise[HttpResponse])](bufferSize, OverflowStrategy.dropNew)
    .via(pool)
    .toMat(Sink.foreach {
      case ((Success(resp), p)) => p.success(resp)
      case ((Failure(e), p)) => p.failure(e)
    })(Keep.left)
    .run

  private val Headers = {
    import HttpEncodingRange.apply
    import akka.http.scaladsl.model.headers.HttpEncodings.{deflate, gzip}
    import akka.http.scaladsl.model.headers.`Accept-Encoding`
    immutable.Seq(`Accept-Encoding`(gzip, deflate))
  }
  protected var enableHttpCompression = false

  private def enableHttpCompressionParam: (String, String) = "enable_http_compression" -> (if (enableHttpCompression) "1" else "0")

  private def readOnlyParam(readOnly: Boolean): (String, String) = "readonly" -> (if (readOnly) "1" else "0")


  def executeRequest(uri: Uri, query: String, readOnly: Boolean = true, entity: Option[RequestEntity] = None): Future[String] = {
    handleResponse(singleRequest(toHttpRequest(uri, query, readOnly, entity)), query)
  }

  def toHttpRequest(uri: Uri, query: String, readOnly: Boolean = true, entity: Option[RequestEntity] = None) = {
    entity match {
      case Some(e) =>
        logger.debug(s"Executing clickhouse query [$query] on host [${uri.toString()}] with entity payload of length ${e.contentLengthOption}")
        HttpRequest(method = HttpMethods.POST, uri =
          uri.withQuery(Query("query" -> query, enableHttpCompressionParam)), entity = e, headers = Headers)
      case None =>
        logger.debug(s"Executing clickhouse query [$query] on host [${uri.toString()}]")
        HttpRequest(method = HttpMethods.POST, uri =
          uri.withQuery(Query(readOnlyParam(readOnly), enableHttpCompressionParam)), entity = query, headers = Headers)
    }
  }

  private def handleResponse(responseFuture: Future[HttpResponse], query: String): Future[String] = {
    responseFuture.flatMap { response =>
      val encoding = response.encoding
      response match {
        case HttpResponse(StatusCodes.OK, _, entity, _) =>
          entityToString(entity, encoding)
        case HttpResponse(code, _, entity, _) =>
          entityToString(entity, encoding).flatMap(response =>
            Future.failed(new ClickhouseException(s"Server returned code $code; $response", query))
          )
      }
    }
  }

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

  private def entityToString(entity: ResponseEntity, encoding: HttpEncoding): Future[String] = {
    entity.dataBytes.runFold(ByteString(""))(_ ++ _).flatMap { byteString =>
      encoding match {
        case HttpEncodings.gzip => Gzip.decode(byteString)
        case _ => Future.successful(byteString)
      }
    }.map(_.utf8String)
  }
}
