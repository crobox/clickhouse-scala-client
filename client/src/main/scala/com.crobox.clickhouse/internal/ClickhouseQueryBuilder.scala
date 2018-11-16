package com.crobox.clickhouse.internal

import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.{HttpEncodingRange, RawHeader}
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, RequestEntity, Uri}
import com.crobox.clickhouse.internal.QuerySettings.ReadQueries
import com.crobox.clickhouse.internal.progress.ProgressHeadersAsEventsStage
import com.typesafe.config.Config
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
                          queryIdentifier: Option[String],
                          settings: QuerySettings,
                          entity: Option[RequestEntity])(config: Config): HttpRequest = {
    val urlQuery = uri.withQuery(Query(Query("query" -> query) ++ settings.withFallback(config).asQueryParams: _*))
    entity match {
      case Some(e) =>
        logger.debug(s"Executing clickhouse query [$query] on host [${uri
          .toString()}] with entity payload of length ${e.contentLengthOption}")
        HttpRequest(
          method = HttpMethods.POST,
          uri = urlQuery,
          entity = e,
          headers = Headers ++ queryIdentifier.map(RawHeader(ProgressHeadersAsEventsStage.InternalQueryIdentifier, _))
        )
      case None
          if settings.idempotent && settings.readOnly == ReadQueries && urlQuery
            .toString()
            .getBytes
            .length < 16 * 1024 => //max url size
        logger.debug(s"Executing clickhouse idempotent query [$query] on host [${uri.toString()}]")
        HttpRequest(
          method = HttpMethods.GET,
          uri = urlQuery.withQuery(
            urlQuery
              .query()
              .filterNot(
                _._1 == "readonly"
              ) //get requests are readonly by default, if we send the readonly flag clickhouse will fail the request
          ),
          headers = Headers ++ queryIdentifier.map(RawHeader(ProgressHeadersAsEventsStage.InternalQueryIdentifier, _))
        )
      case None =>
        logger.debug(s"Executing clickhouse query [$query] on host [${uri.toString()}]")
        HttpRequest(
          method = HttpMethods.POST,
          uri = uri.withQuery(settings.withFallback(config).asQueryParams),
          entity = query,
          headers = Headers ++ queryIdentifier.map(RawHeader(ProgressHeadersAsEventsStage.InternalQueryIdentifier, _))
        )
    }
  }

}
