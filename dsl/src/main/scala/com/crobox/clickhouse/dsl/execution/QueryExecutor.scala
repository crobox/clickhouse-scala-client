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

  def query[V: JsonReader](
      sql: String
  )(implicit executionContext: ExecutionContext, settings: QuerySettings = QuerySettings()): Future[QueryResult[V]]

  def execute[V: JsonReader](
      query: Query
  )(implicit executionContext: ExecutionContext, settings: QuerySettings = QuerySettings()): Future[QueryResult[V]]

  /**
   * Run a query and read the result by column rather than through a hand-written `JsonReader`.
   *
   * Unlike [[execute]] this needs nothing declared up front, so it serves the queries whose select list is only known
   * at runtime -- built from a `Seq[Column]`, a `select(all)`, or widened by `groupBy`. See [[Row]].
   *
   * Named separately from `execute` rather than overloading it: Scala permits default arguments in only one alternative
   * of an overloaded method, and `execute` already has a defaulted `settings`.
   */
  def executeRows(
      query: Query
  )(implicit executionContext: ExecutionContext, settings: QuerySettings = QuerySettings()): Future[QueryResult[Row]]

  def insert[V: JsonWriter](table: Table, values: Seq[V])(implicit
      executionContext: ExecutionContext,
      settings: QuerySettings = QuerySettings()
  ): Future[String]

}
