package com.crobox.clickhouse

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{Framing, Source}
import akka.util.ByteString
import com.crobox.clickhouse.balancing.HostBalancer
import com.crobox.clickhouse.internal.ClickHouseExecutor.QuerySettings
import com.crobox.clickhouse.internal.ClickHouseExecutor.QuerySettings.{AllQueries, ReadQueries}
import com.crobox.clickhouse.internal.{ClickHouseExecutor, ClickhouseQueryBuilder, ClickhouseResponseParser}
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
    override implicit val system: ActorSystem = ActorSystem("clickhouseClient")
) extends ClickHouseExecutor
    with ClickhouseResponseParser
    with ClickhouseQueryBuilder {

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
  def query(sql: String): Future[String] =
    executeRequest(sql, QuerySettings(ReadQueries))

  /**
   * Execute a read-only query on Clickhouse
   *
   * @param sql a valid Clickhouse SQL string
   * @return stream with the query progress (started/rejected/finished/failed) which materializes with the query result
   */
  def queryWithProgress(sql: String): Source[ClickHouseExecutor.QueryProgress, Future[String]] =
    executeRequestWithProgress(sql, QuerySettings(ReadQueries))

  /**
   * Execute a query that is modifying the state of the database. e.g. INSERT, SET, CREATE TABLE.
   * For security purposes SELECT and SHOW queries are not allowed, use the .query() method for those.
   *
   * @param sql a valid Clickhouse SQL string
   * @return Future with the result that clickhouse returns
   */
  def execute(sql: String): Future[String] =
    Future {
      require(
        !(sql.toUpperCase.startsWith("SELECT") || sql.toUpperCase.startsWith("SHOW")),
        ".execute() is not allowed for SELECT or SHOW statements, use .query() instead"
      )
    }.flatMap(
      _ => executeRequest(sql, QuerySettings(AllQueries))
    )

  def execute(sql: String, entity: String): Future[String] =
    executeRequest(sql, QuerySettings(AllQueries), Option(entity))

  /**
   * Creates a stream of the SQL query that will delimit the result from Clickhouse on new-line
   *
   * @param sql a valid Clickhouse SQL string
   */
  def source(sql: String): Source[String, NotUsed] =
    sourceByteString(sql)
      .via(Framing.delimiter(ByteString("\n"), MaximumFrameLength))
      .map(_.utf8String)

  /**
   * Creates a stream of the SQL query that will emit every result as a ByteString
   *
   * @param sql a valid Clickhouse SQL string
   */
  def sourceByteString(sql: String): Source[ByteString, NotUsed] =
    Source
      .fromFuture(hostBalancer.nextHost.flatMap { host =>
        singleRequest(toRequest(host, sql, None, QuerySettings(ReadQueries)))
      })
      .flatMapConcat(response => response.entity.withoutSizeLimit().dataBytes)

  /**
   * Accepts a source of Strings that it will stream to Clickhouse
   *
   * @param sql    a valid Clickhouse SQL INSERT statement
   * @param source the Source with strings
   * @return Future with the result that clickhouse returns
   */
  def sink(sql: String, source: Source[ByteString, Any]): Future[String] = {
    val entity = HttpEntity.apply(ContentTypes.`text/plain(UTF-8)`, source)
    executeRequest(sql, QuerySettings(AllQueries), Option(entity))
  }

  override protected implicit val executionContext: ExecutionContext =
    system.dispatcher
}
