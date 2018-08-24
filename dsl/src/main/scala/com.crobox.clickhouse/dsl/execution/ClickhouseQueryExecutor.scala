package com.crobox.clickhouse.dsl.execution

import akka.stream.scaladsl.{Merge, Source}
import com.crobox.clickhouse.ClickhouseClient
import com.crobox.clickhouse.dsl.language.{ClickhouseTokenizerModule, TokenizerModule}
import com.crobox.clickhouse.dsl.parallel.CumulativeQueries
import com.crobox.clickhouse.dsl.{InternalQuery, OperationalQuery, Query, Table}
import com.crobox.clickhouse.internal.ClickHouseExecutor
import spray.json.{JsonReader, _}

import scala.concurrent.{ExecutionContext, Future}

trait ClickhouseQueryExecutor extends QueryExecutor { self: TokenizerModule =>
  implicit val client: ClickhouseClient

  def execute[V: JsonReader](query: Query)(implicit executionContext: ExecutionContext): Future[QueryResult[V]] =
    query match {
      case _: OperationalQuery if query.internalQuery != null =>
        executeQuery(query.internalQuery)
      //TODO support more than 2 queries
      case CumulativeQueries(first, second) =>
        for {
          res1 <- executeQuery[V](first.internalQuery)
          res2 <- executeQuery[V](second.internalQuery)
        } yield {
          QueryResult(res1.rows ++ res2.rows, None, None)
        }
    }

  def executeWithProgress[V: JsonReader](
      query: Query
  )(implicit executionContext: ExecutionContext): Source[ClickHouseExecutor.QueryProgress, Future[QueryResult[V]]] =
    query match {
      case _: OperationalQuery if query.internalQuery != null =>
        executeQueryWithProgress(query.internalQuery)
      //TODO support more than 2 queries
      case CumulativeQueries(first, second) =>
        Source.combineMat(executeQueryWithProgress[V](first.internalQuery),
                          executeQueryWithProgress[V](second.internalQuery))(Merge(_))(
          (res1, res2) =>
            res1.flatMap(res1Query => {
              res2.map(res2Query => QueryResult(res1Query.rows ++ res2Query.rows, None, None))
            })
        )
    }

  override def insert[V: JsonWriter](table: Table,
                                     values: Seq[V])(implicit executionContext: ExecutionContext): Future[String] =
    Future {
      values.map(_.toJson.compactPrint).mkString("\n") + "\n"
    }.flatMap(
      entity => client.execute(s"INSERT INTO ${client.table(table.name)} FORMAT JSONEachRow", entity)
    )

  private def executeQuery[V: JsonReader](internal: InternalQuery)(implicit executionContext: ExecutionContext,
                                                                   client: ClickhouseClient) = {
    import QueryResult._
    val queryResult = client.query(toSql(internal)(client.database))
    queryResult.map(_.parseJson.convertTo[QueryResult[V]])
  }
  private def executeQueryWithProgress[V: JsonReader](
      internal: InternalQuery
  )(implicit executionContext: ExecutionContext, client: ClickhouseClient) = {
    import QueryResult._
    val queryResult = client.queryWithProgress(toSql(internal)(client.database))
    queryResult.mapMaterializedValue(_.map(_.parseJson.convertTo[QueryResult[V]]))
  }

}

object ClickhouseQueryExecutor {

  def default(clickhouseCLient: ClickhouseClient): ClickhouseQueryExecutor =
    new DefaultClickhouseQueryExecutor(clickhouseCLient)
}

class DefaultClickhouseQueryExecutor(override val client: ClickhouseClient)
    extends ClickhouseQueryExecutor
    with ClickhouseTokenizerModule
