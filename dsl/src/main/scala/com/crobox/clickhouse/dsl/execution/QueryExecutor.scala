package com.crobox.clickhouse.dsl.execution

import com.crobox.clickhouse.ClickhouseServerVersion
import com.crobox.clickhouse.dsl.language.TokenizerModule
import com.crobox.clickhouse.dsl.{Query, Table}
import com.crobox.clickhouse.internal.QuerySettings
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

/**
 * Module that can execute queries, to return a future of result
 */
trait QueryExecutor { self: TokenizerModule =>

  def serverVersion: ClickhouseServerVersion

  def execute[V: JsonReader](query: Query)(implicit executionContext: ExecutionContext,
                                           settings: QuerySettings = QuerySettings()): Future[QueryResult[V]]

  def insert[V: JsonWriter](table: Table, values: Seq[V])(implicit executionContext: ExecutionContext,
                                                          settings: QuerySettings = QuerySettings()): Future[String]

}
