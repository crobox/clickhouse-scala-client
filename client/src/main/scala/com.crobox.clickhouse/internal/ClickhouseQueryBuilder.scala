package com.crobox.clickhouse.internal

import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.HttpEncodingRange
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, RequestEntity, Uri}
import com.crobox.clickhouse.internal.ClickHouseExecutor.QuerySettings
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable

private[clickhouse] trait ClickhouseQueryBuilder extends LazyLogging {

  private val Headers = {
    import HttpEncodingRange.apply
    import akka.http.scaladsl.model.headers.HttpEncodings.{deflate, gzip}
    import akka.http.scaladsl.model.headers.`Accept-Encoding`
    immutable.Seq(`Accept-Encoding`(gzip, deflate))
  }

  protected def toRequest(uri: Uri,
                          query: String,
                          settings: QuerySettings,
                          entity: Option[RequestEntity] = None): HttpRequest =
    entity match {
      case Some(e) =>
        logger.debug(s"Executing clickhouse query [$query] on host [${uri
          .toString()}] with entity payload of length ${e.contentLengthOption}")
        HttpRequest(
          method = HttpMethods.POST,
          uri = uri.withQuery(Query(Query("query" -> query) ++ settings.asQueryParams: _*)),
          entity = e,
          headers = Headers
        )
      case None =>
        logger.debug(s"Executing clickhouse query [$query] on host [${uri.toString()}]")
        HttpRequest(method = HttpMethods.POST,
                    uri = uri.withQuery(settings.asQueryParams),
                    entity = query,
                    headers = Headers)
    }

}
