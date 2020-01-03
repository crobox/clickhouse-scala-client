package com.crobox.clickhouse

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{Framing, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.ByteString
import com.crobox.clickhouse.balancing.HostBalancer
import com.crobox.clickhouse.internal.QuerySettings._
import com.crobox.clickhouse.internal.progress.QueryProgress.QueryProgress
import com.crobox.clickhouse.internal.{ClickHouseExecutor, ClickhouseQueryBuilder, ClickhouseResponseParser, QuerySettings}
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Async clickhouse client using Akka Http and Streams
  *
  * @author Sjoerd Mulder
  * @since 31-03-17
  */
class ClickhouseClient(configuration: Option[Config] = None)
  extends ClickHouseExecutor
    with ClickhouseResponseParser
    with ClickhouseQueryBuilder {

  override protected val config: Config = configuration.getOrElse(ConfigFactory.load())
    .getConfig("crobox.clickhouse.client")
  override protected implicit val system: ActorSystem = ActorSystem("clickhouse-client", config)
  override protected implicit val materializer: Materializer = ActorMaterializer()
  override protected implicit val executionContext: ExecutionContext = system.dispatcher

  override protected val hostBalancer = HostBalancer()

  private val MaximumFrameLength: Int = config.getInt("maximum-frame-length")

  logger.info(s"Starting Clickhouse Client connecting to $hostBalancer")

  /**
    * Execute a read-only query on Clickhouse
    *
    * @param sql a valid Clickhouse SQL string
    * @return Future with the result that clickhouse returns
    */
  def query(sql: String)(implicit settings: QuerySettings = QuerySettings(ReadQueries)): Future[String] =
    executeRequest(sql, settings.copy(readOnly = ReadQueries, idempotent = settings.idempotent.orElse(Some(true))))

  /**
    * Execute a read-only query on Clickhouse
    * Experimental api, may change in the future.
    *
    * @param sql a valid Clickhouse SQL string
    * @return stream with the query progress (started/rejected/finished/failed) which materializes with the query result
    */
  def queryWithProgress(sql: String)(
    implicit settings: QuerySettings = QuerySettings(ReadQueries)
  ): Source[QueryProgress, Future[String]] =
    executeRequestWithProgress(sql,
      settings.copy(readOnly = ReadQueries, idempotent = settings.idempotent.orElse(Some(true))))

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

  def execute(sql: String, entity: String)(implicit settings: QuerySettings): Future[String] =
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
    * It will not retry the queries.
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
    * It will not retry the query as this will run the source once for every retry and might have
    * unexpected consequences.
    *
    * @param sql    a valid Clickhouse SQL INSERT statement
    * @param source the Source with strings
    * @return Future with the result that clickhouse returns
    */
  def sink(sql: String, source: Source[ByteString, Any])(
    implicit settings: QuerySettings = QuerySettings(AllQueries)
  ): Future[String] = {
    val entity = HttpEntity.apply(ContentTypes.`text/plain(UTF-8)`, source)
    executeRequestInternal(hostBalancer.nextHost, sql, queryIdentifier, settings, Option(entity), None)
  }

}
