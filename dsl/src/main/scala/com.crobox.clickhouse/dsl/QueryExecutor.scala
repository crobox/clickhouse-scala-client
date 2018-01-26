package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.ClickhouseClient
import com.crobox.clickhouse.dsl.clickhouse.QueryResult
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

/**
 * Module that can execute queries, to return a future of result
 */
trait QueryExecutor { self: TokenizerModule =>

  def execute[V: JsonReader](query: Query)(implicit executionContext: ExecutionContext,
                                           client: ClickhouseClient): Future[QueryResult[V]]
}
