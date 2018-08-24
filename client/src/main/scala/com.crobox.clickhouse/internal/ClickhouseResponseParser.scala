package com.crobox.clickhouse.internal

import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model.headers.{HttpEncoding, HttpEncodings}
import akka.http.scaladsl.model.{HttpResponse, ResponseEntity, StatusCodes, Uri}
import akka.stream.Materializer
import akka.stream.scaladsl.SourceQueue
import akka.util.ByteString
import com.crobox.clickhouse.ClickhouseException
import com.crobox.clickhouse.internal.ClickHouseExecutor._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private[clickhouse] trait ClickhouseResponseParser {
  protected implicit val executionContext: ExecutionContext
//TODO process the X-Clickhouse-Progress headers and send progress events
  protected def processClickhouseResponse(responseFuture: Future[HttpResponse],
                               query: String,
                               host: Uri,
                               progressQueue: Option[SourceQueue[QueryProgress]])(
      implicit materializer: Materializer
  ): Future[String] =
    responseFuture.flatMap { response =>
      val encoding = response.encoding
      response match {
        case HttpResponse(StatusCodes.OK, _, entity, _) =>
          progressQueue.foreach(_.offer(QueryAccepted))
          entityToString(entity, encoding).andThen {
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
          entityToString(entity, encoding)
            .flatMap(
              response =>
                Future.failed(
                  ClickhouseException(s"Server [$host] returned code $code; $response", query, statusCode = code)
              )
            )
      }
    }

  protected def entityToString(entity: ResponseEntity,
                               encoding: HttpEncoding)(implicit materializer: Materializer): Future[String] =
    entity.dataBytes
      .runFold(ByteString(""))(_ ++ _)
      .flatMap { byteString =>
        encoding match {
          case HttpEncodings.gzip => Gzip.decode(byteString)
          case _                  => Future.successful(byteString)
        }
      }
      .map(_.utf8String)
}
