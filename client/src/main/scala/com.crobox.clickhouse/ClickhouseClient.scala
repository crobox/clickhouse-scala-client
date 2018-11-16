package com.crobox.clickhouse

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{Framing, Source}
import akka.util.ByteString
import com.crobox.clickhouse.balancing.HostBalancer
import com.crobox.clickhouse.internal.QuerySettings._
import com.crobox.clickhouse.internal.progress.QueryProgress.QueryProgress
import com.crobox.clickhouse.internal.{ClickHouseExecutor, ClickhouseQueryBuilder, ClickhouseResponseParser, QuerySettings}
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}

/**
 * Async clickhouse client using Akka Http and Streams
 * TODO remove the implicit ActorSystem and  use own internal actor system + configuration
 *
 * @author Sjoerd Mulder
 * @since 31-03-17
 */
class ClickhouseClient(override val config: Config, val database: String = "default")(
    override implicit val system: ActorSystem = ActorSystem("clickhouseClient", config)
) extends ClickHouseExecutor
    with ClickhouseResponseParser
    with ClickhouseQueryBuilder {

  override protected implicit val executionContext: ExecutionContext = system.dispatcher

  override protected val hostBalancer = HostBalancer(config)

  private val MaximumFrameLength: Int = 1024 * 1024 // 1 MB

  logger.info(s"Starting Clickhouse Client connecting to $hostBalancer, database $database")

  /**
   * Resolves the table name relative to the current clickhouse client database.
   *
   * @param name name of the table
   */
  def table(name: String): String = s"$database.$name"

  /**
   * Execute a read-only query on Clickhouse
   *
   * @param sql a valid Clickhouse SQL string
   * @return Future with the result that clickhouse returns
   */
  def query(sql: String)(implicit settings: QuerySettings = QuerySettings(ReadQueries)): Future[String] =
    executeRequest(sql, settings.copy(readOnly = ReadQueries, idempotent = true))

  /**
   * Execute a read-only query on Clickhouse
   * Experimental api, may change in the future.
   * @param sql a valid Clickhouse SQL string
   * @return stream with the query progress (started/rejected/finished/failed) which materializes with the query result
   */
  def queryWithProgress(sql: String)(
      implicit settings: QuerySettings = QuerySettings(ReadQueries)
  ): Source[QueryProgress, Future[String]] =
    executeRequestWithProgress(sql, settings.copy(readOnly = ReadQueries, idempotent = true))

  /**
   * Execute a query that is modifying the state of the database. e.g. INSERT, SET, CREATE TABLE.
   * For security purposes SELECT and SHOW queries are not allowed, use the .query() method for those.
   *
   * @param sql a valid Clickhouse SQL string
   * @return Future with the result that clickhouse returns
   */
  def execute(sql: String)(implicit settings: QuerySettings = QuerySettings(AllQueries)): Future[String] =
    Future {
      require(
        !(sql.toUpperCase.startsWith("SELECT") || sql.toUpperCase.startsWith("SHOW")),
        ".execute() is not allowed for SELECT or SHOW statements, use .query() instead"
      )
    }.flatMap(
      _ => executeRequest(sql, settings)
    )

  def execute(sql: String,
              entity: String)(implicit settings: QuerySettings): Future[String] =
    executeRequest(sql, settings, Option(entity))

  /**
   * Creates a stream of the SQL query that will delimit the result from Clickhouse on new-line
   *
   * @param sql a valid Clickhouse SQL string
   */
  def source(sql: String)(implicit settings: QuerySettings = QuerySettings(ReadQueries)): Source[String, NotUsed] =
    sourceByteString(sql)
      .via(Framing.delimiter(ByteString("\n"), MaximumFrameLength))
      .map(_.utf8String)

  /**
   * Creates a stream of the SQL query that will emit every result as a ByteString
   *
   * @param sql a valid Clickhouse SQL string
   */
  def sourceByteString(
      sql: String
  )(implicit settings: QuerySettings = QuerySettings(ReadQueries)): Source[ByteString, NotUsed] =
    Source
      .fromFuture(hostBalancer.nextHost.flatMap { host =>
        singleRequest(toRequest(host, sql, None, settings, None)(config))
      })
      .flatMapConcat(response => response.entity.withoutSizeLimit().dataBytes)

  /**
   * Accepts a source of Strings that it will stream to Clickhouse
   *
   * @param sql    a valid Clickhouse SQL INSERT statement
   * @param source the Source with strings
   * @return Future with the result that clickhouse returns
   */
  def sink(sql: String, source: Source[ByteString, Any])(
      implicit settings: QuerySettings = QuerySettings(AllQueries)
  ): Future[String] = {
    val entity = HttpEntity.apply(ContentTypes.`text/plain(UTF-8)`, source)
    executeRequest(sql, settings, Option(entity))
  }

}
