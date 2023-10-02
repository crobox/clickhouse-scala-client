package com.crobox.clickhouse.internal

import org.apache.pekko.http.scaladsl.model.Uri.Query
import org.apache.pekko.http.scaladsl.model.headers.HttpEncoding
import com.crobox.clickhouse.internal.QuerySettings._
import com.typesafe.config.Config

//import scala.jdk.CollectionConverters._
import scala.collection.JavaConverters._
import scala.util.Try

case class QuerySettings(readOnly: ReadOnlySetting = AllQueries,
                         authentication: Option[(String, String)] = None,
                         progressHeaders: Option[Boolean] = None,
                         queryId: Option[String] = None,
                         profile: Option[String] = None,
                         httpCompression: Option[Boolean] = None,
                         settings: Map[String, String] = Map.empty,
                         idempotent: Option[Boolean] = None,
                         retries: Option[Int] = None,
                         requestCompressionType: Option[HttpEncoding] = None) {

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

  def withFallback(config: Config): QuerySettings = {
    val custom = config.getConfig(path("custom"))
    this.copy(
      authentication = authentication.orElse(Try {
        val authConfig = config.getConfig(path("authentication"))
        (authConfig.getString("user"), authConfig.getString("password"))
      }.toOption),
      profile = profile.orElse(Try { config.getString(path("profile")) }.toOption),
      httpCompression = httpCompression.orElse(Try { config.getBoolean(path("http-compression")) }.toOption),
      settings = custom.entrySet().asScala.map(u => (u.getKey, custom.getString(u.getKey))).toMap
      ++ settings
    )
  }

  private def path(setting: String) = s"settings.$setting"
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
