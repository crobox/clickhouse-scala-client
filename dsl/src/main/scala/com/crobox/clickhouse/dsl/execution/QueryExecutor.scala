package com.crobox.clickhouse.dsl.execution

import com.crobox.clickhouse.dsl.language.TokenizerModule
import com.crobox.clickhouse.dsl.{ExplainKind, Query, Statement, Table}
import com.crobox.clickhouse.internal.QuerySettings
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

/**
 * Module that can execute queries, to return a future of result
 */
trait QueryExecutor { self: TokenizerModule =>

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

  /**
   * `EXPLAIN`, as the lines of text it reports. Empty for [[ExplainKind.Estimate]], whose result is a set of columns
   * rather than a single `explain` one -- use [[explainRows]] for that.
   */
  def explain(
      kind: ExplainKind,
      query: Query,
      options: Seq[(String, String)] = Seq.empty
  )(implicit executionContext: ExecutionContext, settings: QuerySettings = QuerySettings()): Future[Seq[String]]

  /**
   * `EXPLAIN` read by column, for kinds whose result is not a single `explain` column. Mirrors [[executeRows]]: the
   * column set depends on the kind, so there is nothing to declare up front.
   */
  def explainRows(
      kind: ExplainKind,
      query: Query,
      options: Seq[(String, String)] = Seq.empty
  )(implicit executionContext: ExecutionContext, settings: QuerySettings = QuerySettings()): Future[QueryResult[Row]]

  /**
   * Run a data-changing statement -- see [[com.crobox.clickhouse.dsl.Statement]] -- and return the server's response
   * body, which is empty on success. Mirrors [[insert]] rather than the `execute`/`executeRows` pair: there are no rows
   * to read back.
   *
   * A mutation (`ALTER TABLE ... DELETE`/`UPDATE`) returns as soon as it is queued, not when it has finished; pass
   * `mutations_sync` through `settings` to wait, and watch `system.mutations` otherwise.
   */
  def executeStatement(statement: Statement)(implicit
      executionContext: ExecutionContext,
      settings: QuerySettings = QuerySettings()
  ): Future[String]

  def insert[V: JsonWriter](table: Table, values: Seq[V])(implicit
      executionContext: ExecutionContext,
      settings: QuerySettings = QuerySettings()
  ): Future[String]

}
