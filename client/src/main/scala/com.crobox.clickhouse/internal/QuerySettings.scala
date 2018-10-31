package com.crobox.clickhouse.internal
import akka.http.scaladsl.model.Uri.Query
import com.crobox.clickhouse.internal.QuerySettings._
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.util.Try
case class QuerySettings(readOnly: ReadOnlySetting = AllQueries,
                         authentication: Option[(String, String)] = None,
                         progressHeaders: Option[Boolean] = None,
                         queryId: Option[String] = None,
                         profile: Option[String] = None,
                         httpCompression: Option[Boolean] = None,
                         settings: Map[String, String] = Map.empty) {

  def asQueryParams: Query =
    Query(
      settings ++ (Seq("readonly" -> readOnly.value.toString) ++
      queryId.map("query_id"      -> _) ++
      authentication.map(
        auth => "user" -> auth._1
      ) ++
      authentication.map(auth => "password" -> auth._2) ++
      profile.map("profile" -> _) ++
      progressHeaders.map(
        progress => "send_progress_in_http_headers" -> (if (progress) "1" else "0")
      ) ++
      httpCompression
        .map(compression => "enable_http_compression" -> (if (compression) "1" else "0"))).toMap
    )

  def withFallback(config: Config): QuerySettings =
    this.copy(
      authentication = authentication.orElse(Try {
        val authConfig = config.getConfig(path("authentication"))
        (authConfig.getString("user"), authConfig.getString("password"))
      }.toOption),
      profile = profile.orElse(Try { config.getString(path("profile")) }.toOption),
      httpCompression = httpCompression.orElse(Try { config.getBoolean(path("http-compression")) }.toOption),
      settings = config
        .getValue(path("custom"))
        .unwrapped()
        .asInstanceOf[java.util.Map[String, String]]
        .asScala
        .toMap ++ settings
    )

  private def path(setting: String) = s"crobox.clickhouse.client.settings.$setting"

}

object QuerySettings {
  sealed trait ReadOnlySetting {
    val value: Int
  }
  case object AllQueries extends ReadOnlySetting {
    override val value: Int = 0
  }
  case object ReadQueries extends ReadOnlySetting {
    override val value: Int = 1
  }
  case object ReadAndChangeQueries extends ReadOnlySetting {
    override val value: Int = 2
  }
}
