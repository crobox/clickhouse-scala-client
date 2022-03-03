package com.crobox.clickhouse

import com.crobox.clickhouse.dsl.Query
import com.crobox.clickhouse.internal.QuerySettings
import org.scalatest.Suite
import org.scalatest.time.{Millis, Seconds, Span}
import spray.json.DefaultJsonProtocol.{IntJsonFormat, StringJsonFormat, jsonFormat}
import spray.json.RootJsonFormat

import scala.concurrent.Future

trait DslITSpec extends DslIntegrationSpec {
  this: Suite =>

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(20, Millis)))

  def execute(query: Query): Future[String] = {
    implicit val settings: QuerySettings = QuerySettings()
    clickClient.query(toSql(query.internalQuery, None)).map(_.trim)
  }

  case class StringResult(result: String)
  implicit val stringResultFormat: RootJsonFormat[StringResult] =
    jsonFormat[String, StringResult](StringResult.apply, "result")

  case class IntResult(result: Int)
  implicit val intResultFormat: RootJsonFormat[IntResult] =
    jsonFormat[Int, IntResult](IntResult.apply, "result")
}
