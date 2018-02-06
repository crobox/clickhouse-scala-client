package com.crobox.clickhouse.dsl.execution

import com.crobox.clickhouse.ClickhouseClient
import com.crobox.clickhouse.dsl.language.{ClickhouseTokenizerModule, TokenizerModule}
import com.crobox.clickhouse.dsl.parallel.CumulativeQueries
import com.crobox.clickhouse.dsl.{OperationalQuery, Query, Table, TableColumn, UnderlyingQuery}
import com.typesafe.scalalogging.LazyLogging
import spray.json.{JsonReader, _}

import scala.concurrent.{ExecutionContext, Future}

trait ClickhouseQueryExecutor extends QueryExecutor { self: TokenizerModule =>
  implicit val client: ClickhouseClient

  def execute[V: JsonReader](query: Query)(implicit executionContext: ExecutionContext): Future[QueryResult[V]] =
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

  override def insert[V: JsonWriter](table: Table,
                                     values: Seq[V])(implicit executionContext: ExecutionContext): Future[String] =
    Future {
      values.map(_.toJson.compactPrint).mkString("\n") + "\n"
    }.flatMap(
      entity => client.execute(s"INSERT INTO ${client.table(table.name)} FORMAT JSONEachRow", entity)
    )

  private def executeQuery[V: JsonReader](underlying: UnderlyingQuery)(implicit executionContext: ExecutionContext,
                                                                       client: ClickhouseClient) = {
    import QueryResult._
    val queryResult = client.query(toSql(underlying)(client.database))
    queryResult.map(_.parseJson.convertTo[QueryResult[V]])
  }

}

object ClickhouseQueryExecutor {

  def default(clickhouseCLient: ClickhouseClient): ClickhouseQueryExecutor =
    new DefaultClickhouseQueryExecutor(clickhouseCLient)
}

class DefaultClickhouseQueryExecutor(override val client: ClickhouseClient)
    extends ClickhouseQueryExecutor
    with ClickhouseTokenizerModule
