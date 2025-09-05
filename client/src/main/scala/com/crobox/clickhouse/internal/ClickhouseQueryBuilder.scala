package com.crobox.clickhouse.internal

import org.apache.pekko.http.scaladsl.model.Uri.Query
import org.apache.pekko.http.scaladsl.model.headers.{`Content-Encoding`, HttpEncodingRange, RawHeader}
import org.apache.pekko.http.scaladsl.model.{HttpMethods, HttpRequest, RequestEntity, Uri}
import com.crobox.clickhouse.internal.QuerySettings.ReadQueries
import com.crobox.clickhouse.internal.progress.ProgressHeadersAsEventsStage
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable

private[clickhouse] trait ClickhouseQueryBuilder extends LazyLogging {

  private val Headers = {
    import HttpEncodingRange.apply
    import org.apache.pekko.http.scaladsl.model.headers.HttpEncodings.{deflate, gzip}
    import org.apache.pekko.http.scaladsl.model.headers.`Accept-Encoding`
    immutable.Seq(`Accept-Encoding`(gzip, deflate))
  }
  private val MaxUriSize = 16 * 1024

  protected def toRequest(
      uri: Uri,
      query: String,
      queryIdentifier: Option[String],
      settings: QuerySettings,
      entity: Option[RequestEntity]
  )(config: Config): HttpRequest = {
    val settingsWithFallback = settings.withFallback(config)
    val urlQuery = uri.withQuery(Query(Query("query" -> query) ++ settingsWithFallback.asQueryParams: _*))
    entity match {
      case Some(e) =>
        logger.debug(
          s"Executing clickhouse query [$query] on host [${uri.toString()}] with entity payload of length ${e.contentLengthOption}"
        )
        HttpRequest(
          method = HttpMethods.POST,
          uri = urlQuery,
          entity = e,
          headers = Headers ++
            queryIdentifier.map(RawHeader(ProgressHeadersAsEventsStage.InternalQueryIdentifier, _)) ++
            settings.requestCompressionType.map(`Content-Encoding`(_)) ++
            (if (settingsWithFallback.sslCertAuth.contains(true))
              Seq(RawHeader("X-ClickHouse-SSL-Certificate-Auth", "on")) ++
                settingsWithFallback.authentication.map(auth => RawHeader("X-ClickHouse-User", auth._1))
            else Seq.empty)
        )
      case None
          if settings.idempotent.contains(true)
            && settings.readOnly == ReadQueries
            && urlQuery.toString().getBytes.length < MaxUriSize => // max url size
        logger.debug(s"Executing clickhouse idempotent query [$query] on host [${uri.toString()}]")
        HttpRequest(
          method = HttpMethods.GET,
          uri = urlQuery.withQuery(
            urlQuery
              .query()
              .filterNot(
                _._1 == "readonly"
              ) // get requests are readonly by default, if we send the readonly flag clickhouse will fail the request
          ),
          headers = Headers ++ queryIdentifier.map(RawHeader(ProgressHeadersAsEventsStage.InternalQueryIdentifier, _)) ++
            (if (settingsWithFallback.sslCertAuth.contains(true))
              Seq(RawHeader("X-ClickHouse-SSL-Certificate-Auth", "on")) ++
                settingsWithFallback.authentication.map(auth => RawHeader("X-ClickHouse-User", auth._1))
            else Seq.empty)
        )
      case None =>
        logger.debug(s"Executing clickhouse query [$query] on host [${uri.toString()}]")
        HttpRequest(
          method = HttpMethods.POST,
          uri = uri.withQuery(settingsWithFallback.asQueryParams),
          entity = query,
          headers = Headers ++ queryIdentifier.map(RawHeader(ProgressHeadersAsEventsStage.InternalQueryIdentifier, _))
        )
    }
  }
}
