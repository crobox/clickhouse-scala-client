package com.crobox.clickhouse.internal

import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.HttpEncodingRange
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, RequestEntity, Uri}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable

private[clickhouse] trait ClickhouseQueryBuilder extends LazyLogging {

  protected def config: Config
  private val Headers = {
    import HttpEncodingRange.apply
    import akka.http.scaladsl.model.headers.HttpEncodings.{deflate, gzip}
    import akka.http.scaladsl.model.headers.`Accept-Encoding`
    immutable.Seq(`Accept-Encoding`(gzip, deflate))
  }

  protected def toRequest(uri: Uri,
                          query: String,
                          readOnly: Boolean = true,
                          entity: Option[RequestEntity] = None): HttpRequest = {
    entity match {
      case Some(e) =>
        logger.debug(s"Executing clickhouse query [$query] on host [${uri
          .toString()}] with entity payload of length ${e.contentLengthOption}")
        HttpRequest(
          method = HttpMethods.POST,
          uri =
            uri.withQuery(Query("query" -> query, enableHttpCompressionParam)),
          entity = e,
          headers = Headers)
      case None =>
        logger.debug(
          s"Executing clickhouse query [$query] on host [${uri.toString()}]")
        HttpRequest(
          method = HttpMethods.POST,
          uri = uri.withQuery(
            Query(readOnlyParam(readOnly), enableHttpCompressionParam)),
          entity = query,
          headers = Headers)
    }
  }

  private def enableHttpCompressionParam: (String, String) =
    "enable_http_compression" -> (if (config.getBoolean("com.crobox.clickhouse.client.httpCompression")) "1" else "0")

  private def readOnlyParam(readOnly: Boolean): (String, String) =
    "readonly" -> (if (readOnly) "1" else "0")

}
