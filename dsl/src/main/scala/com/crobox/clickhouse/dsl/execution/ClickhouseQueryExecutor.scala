package com.crobox.clickhouse.dsl.execution

import com.crobox.clickhouse.ClickhouseClient
import com.crobox.clickhouse.dsl.language.{ClickhouseTokenizerModule, TokenizeContext, TokenizerModule}
import com.crobox.clickhouse.dsl.{ExplainKind, Query, Table}
import com.crobox.clickhouse.internal.QuerySettings
import spray.json.{JsonReader, _}

import scala.concurrent.{ExecutionContext, Future}

trait ClickhouseQueryExecutor extends QueryExecutor {
  self: TokenizerModule =>
  implicit val client: ClickhouseClient

  override def execute[V: JsonReader](
      query: Query
  )(implicit executionContext: ExecutionContext, settings: QuerySettings = QuerySettings()): Future[QueryResult[V]] = {
    import QueryResult._
    val queryResult = client.query(toSql(query.internalQuery)(ctx = TokenizeContext()))(settings)
    queryResult.map(_.parseJson.convertTo[QueryResult[V]])
  }

  override def query[V: JsonReader](
      sql: String
  )(implicit executionContext: ExecutionContext, settings: QuerySettings = QuerySettings()): Future[QueryResult[V]] = {
    import QueryResult._
    val queryResult = client.query(sql)(settings)
    queryResult.map(_.parseJson.convertTo[QueryResult[V]])
  }

  //  override def execute[V: JsonReader](
//      sql: String
//  )(implicit executionContext: ExecutionContext, settings: QuerySettings = QuerySettings()): Future[QueryResult[V]] =
//    client.query(sql).map(_.parseJson.convertTo[QueryResult[V]])

  //  def executeWithProgress[V: JsonReader](
  //      query: Query
  //  )(implicit executionContext: ExecutionContext,
  //    settings: QuerySettings = QuerySettings()): Source[QueryProgress, Future[QueryResult[V]]] = {
  //    import QueryResult._
  //    val queryResult =
  //      client.queryWithProgress(toSql(query.internalQuery)(ctx = TokenizeContext()))
  //    queryResult.mapMaterializedValue(_.map(_.parseJson.convertTo[QueryResult[V]]))
  //  }

  /** Every EXPLAIN kind but ESTIMATE reports a single column under this name. */
  private val ExplainColumn = "explain"

  override def explain(
      kind: ExplainKind,
      query: Query,
      options: Seq[(String, String)] = Seq.empty
  )(implicit
      executionContext: ExecutionContext,
      settings: QuerySettings = QuerySettings()
  ): Future[Seq[String]] =
    explainRows(kind, query, options).map(_.rows.flatMap(_.getByName[String](ExplainColumn)))

  override def explainRows(
      kind: ExplainKind,
      query: Query,
      options: Seq[(String, String)] = Seq.empty
  )(implicit
      executionContext: ExecutionContext,
      settings: QuerySettings = QuerySettings()
  ): Future[QueryResult[Row]] = {
    val explainSql = toExplainSql(kind, query.internalQuery, options)(ctx = TokenizeContext())
    // Selected from as a subquery rather than given a trailing FORMAT. EXPLAIN AST reports the whole parsed statement,
    // so a `FORMAT` after it becomes part of what it explains and the result comes back in the default format instead;
    // wrapping sidesteps that and reads identically for every kind.
    val sql = s"SELECT * FROM ($explainSql) FORMAT ${CompactRowParser.Format}"
    client.query(sql)(settings).map(CompactRowParser.parse)
  }

  override def executeRows(
      query: Query
  )(implicit
      executionContext: ExecutionContext,
      settings: QuerySettings = QuerySettings()
  ): Future[QueryResult[Row]] = {
    val sql = toSql(query.internalQuery, formatting = Option(CompactRowParser.Format))(ctx = TokenizeContext())
    client.query(sql)(settings).map(CompactRowParser.parse)
  }

  override def insert[V: JsonWriter](
      table: Table,
      values: Seq[V]
  )(implicit executionContext: ExecutionContext, settings: QuerySettings = QuerySettings()): Future[String] =
    Future {
      values.map(_.toJson.compactPrint).mkString("\n") + "\n"
    }.flatMap(entity => client.execute(s"INSERT INTO ${table.quoted} FORMAT JSONEachRow", entity)(settings))
}

object ClickhouseQueryExecutor {

  def default(clickhouseClient: ClickhouseClient): QueryExecutor =
    new DefaultClickhouseQueryExecutor(clickhouseClient)
}

class DefaultClickhouseQueryExecutor(override val client: ClickhouseClient)
    extends ClickhouseQueryExecutor
    with ClickhouseTokenizerModule
