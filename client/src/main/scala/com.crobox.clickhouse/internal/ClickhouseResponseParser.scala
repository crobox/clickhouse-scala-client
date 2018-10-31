package com.crobox.clickhouse.internal

import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{HttpEncoding, HttpEncodings}
import akka.stream.Materializer
import akka.stream.scaladsl.SourceQueue
import akka.util.ByteString
import com.crobox.clickhouse.internal.progress.QueryProgress.{QueryProgress, _}
import com.crobox.clickhouse.{ClickhouseChunkedException, ClickhouseException}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private[clickhouse] trait ClickhouseResponseParser {

  protected def processClickhouseResponse(responseFuture: Future[HttpResponse],
                                          query: String,
                                          host: Uri,
                                          progressQueue: Option[SourceQueue[QueryProgress]])(
      implicit materializer: Materializer,
      executionContext: ExecutionContext
  ): Future[String] =
    responseFuture.flatMap { response =>
      val encoding = response.encoding
      response match {
        case HttpResponse(StatusCodes.OK, _, entity, _) =>
          entityToString(entity, encoding, progressQueue)
            .map(content => {
              if (content.contains("DB::Exception")) { //FIXME this is quite a fragile way to detect failures, hopefully nobody will have a valid exception string in the result. Check https://github.com/yandex/ClickHouse/issues/2999
                throw ClickhouseException("Found exception in the query return body",
                                          query,
                                          ClickhouseChunkedException(content),
                                          StatusCodes.OK)
              }
              content
            })
            .andThen {
              case Success(_) =>
                progressQueue.foreach(queue => {
                  queue.offer(QueryFinished)
                })
              case Failure(exception) =>
                progressQueue.foreach(queue => {
                  queue.offer(QueryFailed(exception))
                })
            }
        case HttpResponse(code, _, entity, _) =>
          progressQueue.foreach(_.offer(QueryRejected))
          entityToString(entity, encoding, progressQueue)
            .flatMap(
              response =>
                Future.failed(
                  ClickhouseException(s"Server [$host] returned code $code; $response", query, statusCode = code)
              )
            )
      }
    }

  protected def entityToString(
      entity: ResponseEntity,
      encoding: HttpEncoding,
      progressQueue: Option[SourceQueue[QueryProgress]]
  )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[String] =
    entity.dataBytes
      .runFold(ByteString(""))(_ ++ _)
      .flatMap { byteString =>
        encoding match {
          case HttpEncodings.gzip => Gzip.decode(byteString)
          case _                  => Future.successful(byteString)
        }
      }
      .map(_.utf8String)

  protected def splitResponse(response: String): Seq[String] =
    response.split("\n").toSeq
}
