package com.crobox.clickhouse

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{Framing, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.ByteString
import com.crobox.clickhouse.balancing.{QueryBalancer, SingleHostQueryBalancer}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * Async clickhouse client using Akka Http and Streams
  * TODO remove the implicit ActorSystem and  use own internal actor system + configuration
  *
  * @author Sjoerd Mulder
  * @since 31-03-17
  */
class ClickhouseClient(hostBalancer: QueryBalancer, val database: String = "default",
                       override val bufferSize: Int = 1024)(override implicit val system: ActorSystem) extends LazyLogging with ClickHouseExecutor {

  def this(host: String, database: String)(implicit system: ActorSystem) {
    this(SingleHostQueryBalancer(host), database)
  }

  import system.dispatcher

  override implicit val materializer: Materializer = ActorMaterializer()

  private val MaximumFrameLength: Int = 1024 * 1024 // 1 MB

  logger.info(s"Starting Clickhouse Client connecting to $hostBalancer, database $database")
  validateConnection()

  private def validateConnection(): Unit = {
    Await.ready(query("SELECT 1"), 10.seconds).failed.foreach {
      _: Throwable => logger.error(s"Server is unavailable! Couldn't connect to $hostBalancer")
    }
  }

  /**
    * Resolves the table name relative to the current clickhouse client database.
    *
    * @param name name of the table
    */
  def table(name: String): String = s"$database.$name"

  /**
    * Enables http compression (enable_http_compression) when communicating with Clickhouse
    */
  def withHttpCompression(): ClickhouseClient = {
    enableHttpCompression = true
    this
  }


  /**
    * Execute a read-only query on Clickhouse
    *
    * @param sql a valid Clickhouse SQL string
    * @return Future with the result that clickhouse returns
    */
  def query(sql: String): Future[String] = {
    executeWithRetries {
      executeRequest(_, sql)
    }
  }

  /**
    * Execute a query that is modifying the state of the database. e.g. INSERT, SET, CREATE TABLE.
    * For security purposes SELECT and SHOW queries are not allowed, use the .query() method for those.
    *
    * @param sql a valid Clickhouse SQL string
    * @return Future with the result that clickhouse returns
    */
  def execute(sql: String): Future[String] = {
    require(!(sql.toUpperCase.startsWith("SELECT") || sql.toUpperCase.startsWith("SHOW")),
      ".execute() is not allowed for SELECT or SHOW statements, use .query() instead")
    executeWithRetries {
      executeRequest(_, sql, readOnly = false)
    }
  }

  def execute(sql: String, entity: String): Future[String] = {
    executeWithRetries {
      executeRequest(_, sql, readOnly = false, Option(entity))
    }
  }

  /**
    * Creates a stream of the SQL query that will delimit the result from Clickhouse on new-line
    *
    * @param sql a valid Clickhouse SQL string
    */
  def source(sql: String): Source[String, NotUsed] = {
    Source.fromFuture(singleRequest(toHttpRequest(hostBalancer.nextHost, sql)))
      .flatMapConcat(response => response.entity.withoutSizeLimit().dataBytes)
      .via(Framing.delimiter(ByteString("\n"), MaximumFrameLength))
      .map(_.utf8String)
  }


  /**
    * Accepts a source of Strings that it will stream to Clickhouse
    *
    * @param sql    a valid Clickhouse SQL INSERT statement
    * @param source the Source with strings
    * @return Future with the result that clickhouse returns
    */
  def sink(sql: String, source: Source[ByteString, Any]): Future[String] = {
    val entity = HttpEntity.apply(ContentTypes.`text/plain(UTF-8)`, source)
    executeWithRetries {
      executeRequest(_, sql, readOnly = false, Option(entity))
    }
  }

  private def executeWithRetries(request: Uri => Future[String], retries: Int = 5): Future[String] = {
    request(hostBalancer.nextHost).recoverWith {
      // The http server closed the connection unexpectedly before delivering responses for 1 outstanding requests
      case e: RuntimeException if e.getMessage.contains("The http server closed the connection unexpectedly") && retries > 0 =>
        logger.warn(s"Unexpected connection closure, retries left: $retries", e)
        //Retry the request with 1 less retry
        executeWithRetries(request, retries - 1)
    }
  }


}
object ClickhouseClient {
  implicit def hostToBalancer(host: String): QueryBalancer = SingleHostQueryBalancer(host)
}
