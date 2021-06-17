package com.crobox.clickhouse.internal

import akka.http.scaladsl.coding.{Deflate, Gzip, NoCoding}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{HttpEncoding, HttpEncodings}
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.Materializer
import akka.stream.scaladsl.SourceQueue
import com.crobox.clickhouse.internal.progress.QueryProgress.{QueryProgress, _}
import com.crobox.clickhouse.{ClickhouseChunkedException, ClickhouseException}

import scala.concurrent.{ExecutionContext, Future}

private[clickhouse] trait ClickhouseResponseParser {

  protected def processClickhouseResponse(responseFuture: Future[HttpResponse],
                                          query: String,
                                          host: Uri,
                                          progressQueue: Option[SourceQueue[QueryProgress]])(
      implicit materializer: Materializer,
      executionContext: ExecutionContext
  ): Future[String] =
    responseFuture.flatMap { response =>
      decodeResponse(response) match {
        case HttpResponse(StatusCodes.OK, _, entity, _) =>
          progressQueue match {
            case Some(queue) =>
              queue.offer(QueryFinished(entity))
              Future.successful("Result is in the QueryProgress stream")
            case None =>
              Unmarshaller
                .stringUnmarshaller(entity)
                .map(content => {
                  //FIXME this is quite a fragile way to detect failures, hopefully nobody will have a
                  // valid exception string in the result. Check https://github.com/yandex/ClickHouse/issues/2999
                  if (content.contains("DB::Exception")) {
                    throw ClickhouseException("Found exception in the query return body",
                                              query,
                                              ClickhouseChunkedException(content),
                                              StatusCodes.OK)
                  }
                  content
                })
          }
        case HttpResponse(code, _, entity, _) =>
          Unmarshaller
            .stringUnmarshaller(entity)
            .flatMap(response => {
              val exception =
                ClickhouseException(s"Server [$host] returned code $code; $response", query, statusCode = code)
              progressQueue.foreach(_.offer(QueryFailed(exception)))
              Future.failed(exception)
            })
      }
    }

  protected def decodeResponse(response: HttpResponse): HttpResponse = {
    val decoder = response.encoding match {
      case HttpEncodings.gzip     => Gzip
      case HttpEncodings.deflate  => Deflate
      case HttpEncodings.identity => NoCoding
      case HttpEncoding(enc)      => throw new IllegalArgumentException(s"Unsupported response encoding: $enc")
    }
    decoder.decodeMessage(response)
  }

  protected def splitResponse(response: String): Seq[String] =
    response.split("\n").toSeq
}
