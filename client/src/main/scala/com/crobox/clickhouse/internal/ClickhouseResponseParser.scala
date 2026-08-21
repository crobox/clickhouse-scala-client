package com.crobox.clickhouse.internal

import org.apache.pekko.http.scaladsl.coding.Coders
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.model.headers.{HttpEncoding, HttpEncodings}
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshaller
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.SourceQueue
import com.crobox.clickhouse.internal.progress.QueryProgress._
import com.crobox.clickhouse.{ClickhouseChunkedException, ClickhouseException}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private[clickhouse] object ClickhouseResponseParser {

  /**
   * ClickHouse cannot report a failure in the status line once it has started streaming: a query that fails part way
   * through its result still returns 200, without an `X-ClickHouse-Exception-Code` header, and appends the exception to
   * the body. So the body is the only place that failure can be seen, and it has to be inspected.
   *
   * Matched on the shape ClickHouse actually appends -- `Code: <n>. DB::Exception` -- rather than a bare
   * "DB::Exception" substring, so that a result row which merely contains that text is not mistaken for a failure. That
   * was the concern recorded in https://github.com/ClickHouse/ClickHouse/issues/2999. Matching the whole body rather
   * than a trailing window, since an exception message has no bounded length.
   */
  private[internal] val StreamedExceptionMarker = """Code: (\d+)\. DB::Exception""".r
}

private[clickhouse] trait ClickhouseResponseParser {
  import ClickhouseResponseParser.StreamedExceptionMarker

  protected def processClickhouseResponse(
      responseFuture: Future[HttpResponse],
      query: String,
      host: Uri,
      progressQueue: Option[SourceQueue[QueryProgress]]
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext
  ): Future[String] =
    responseFuture.flatMap { response =>
      decodeResponse(response) match {
        case HttpResponse(StatusCodes.OK, _, entity, _) =>
          Unmarshaller
            .stringUnmarshaller(entity)
            .map { content =>
              StreamedExceptionMarker.findFirstMatchIn(content).foreach { marker =>
                throw ClickhouseException(
                  s"Query failed after the response had started streaming; ClickHouse error code ${marker.group(1)}",
                  query,
                  ClickhouseChunkedException(content),
                  StatusCodes.OK
                )
              }
              content
            }
            .andThen {
              case Success(_) =>
                progressQueue.foreach(queue => queue.offer(QueryFinished))
              case Failure(exception) =>
                progressQueue.foreach(queue => queue.offer(QueryFailed(exception)))
            }
        case HttpResponse(code, _, entity, _) =>
          progressQueue.foreach(_.offer(QueryRejected))
          Unmarshaller
            .stringUnmarshaller(entity)
            .flatMap(response =>
              Future.failed(
                ClickhouseException(s"Server [$host] returned code $code; $response", query, statusCode = code)
              )
            )
      }
    }

  protected def decodeResponse(response: HttpResponse): HttpResponse = {
    val decoder = response.encoding match {
      case HttpEncodings.gzip     => Coders.Gzip
      case HttpEncodings.deflate  => Coders.Deflate
      case HttpEncodings.identity => Coders.NoCoding
      case HttpEncoding(enc)      => throw new IllegalArgumentException(s"Unsupported response encoding: $enc")
    }
    decoder.decodeMessage(response)
  }

  protected def splitResponse(response: String): Seq[String] =
    response.split("\n").toSeq
}
