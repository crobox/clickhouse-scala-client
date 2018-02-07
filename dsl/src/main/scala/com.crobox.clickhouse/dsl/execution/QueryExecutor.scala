package com.crobox.clickhouse.dsl.execution

import com.crobox.clickhouse.dsl.language.TokenizerModule
import com.crobox.clickhouse.dsl.{Query, Table}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

/**
 * Module that can execute queries, to return a future of result
 */
trait QueryExecutor { self: TokenizerModule =>

  def execute[V: JsonReader](query: Query)(implicit executionContext: ExecutionContext): Future[QueryResult[V]]

  def insert[V: JsonWriter](table: Table, values: Seq[V])(implicit executionContext: ExecutionContext): Future[String]
}
