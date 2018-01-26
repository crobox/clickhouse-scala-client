package com.crobox.clickhouse.dsl.clickhouse

import com.crobox.clickhouse.ClickhouseClient
import com.crobox.clickhouse.dsl.parallel.CumulativeQueries
import com.crobox.clickhouse.dsl.{OperationalQuery, Query, QueryExecutor, TokenizerModule, UnderlyingQuery}
import com.typesafe.scalalogging.LazyLogging
import spray.json.{JsonReader, _}

import scala.concurrent.{ExecutionContext, Future}

trait ClickhouseQueryExecutor extends QueryExecutor with LazyLogging { self: TokenizerModule =>

  def execute[V: JsonReader](query: Query)(implicit executionContext: ExecutionContext,
                                           client: ClickhouseClient): Future[QueryResult[V]] =
    query match {
      case _: OperationalQuery if query.underlying != null =>
        executeQuery(query.underlying)
      //TODO support more than 2 queries
      case CumulativeQueries(first, second) =>
        for {
          res1 <- executeQuery[V](first.underlying)
          res2 <- executeQuery[V](second.underlying)
        } yield {
          QueryResult(res1.rows ++ res2.rows, None, None)
        }
    }

  private def executeQuery[V: JsonReader](underlying: UnderlyingQuery)(implicit executionContext: ExecutionContext,
                                                                       client: ClickhouseClient) = {
    import QueryResult._
    val queryResult = client.query(toSql(underlying)(client.database))
    queryResult.map(_.parseJson.convertTo[QueryResult[V]])
  }

}
