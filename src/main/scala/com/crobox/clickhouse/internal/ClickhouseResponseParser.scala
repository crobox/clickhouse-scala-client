package com.crobox.clickhouse.internal

import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model.headers.{HttpEncoding, HttpEncodings}
import akka.http.scaladsl.model.{HttpResponse, ResponseEntity, StatusCodes}
import akka.stream.Materializer
import akka.util.ByteString
import com.crobox.clickhouse.ClickhouseException

import scala.concurrent.{ExecutionContext, Future}

private[clickhouse] trait ClickhouseResponseParser {
  protected implicit val executionContext: ExecutionContext

  protected def handleResponse(
      responseFuture: Future[HttpResponse],
      query: String)(implicit materializer: Materializer): Future[String] = {
    responseFuture.flatMap { response =>
      val encoding = response.encoding
      response match {
        case HttpResponse(StatusCodes.OK, _, entity, _) =>
          entityToString(entity, encoding)
        case HttpResponse(code, _, entity, _) =>
          entityToString(entity, encoding).flatMap(response =>
            Future.failed(
              new ClickhouseException(s"Server returned code $code; $response",
                                      query)))
      }
    }
  }

  protected def entityToString(entity: ResponseEntity, encoding: HttpEncoding)(
      implicit materializer: Materializer): Future[String] = {
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
}
