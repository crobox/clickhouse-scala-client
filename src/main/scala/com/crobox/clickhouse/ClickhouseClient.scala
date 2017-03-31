package com.crobox.clickhouse

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{HttpEncoding, HttpEncodingRange, HttpEncodings}
import akka.stream.scaladsl.{Framing, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer, OverflowStrategy, QueueOfferResult}
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success}

/**
  * Async clickhouse client using Akka Http and Streams
  * TODO remove the implicit ActorSystem and  use own internal actor system + configuration
  *
  * @author Sjoerd Mulder
  * @since 31-03-17
  */
class ClickhouseClient(host: String, val database: String = "default",
                       bufferSize: Int = 1024)(implicit val system: ActorSystem) extends LazyLogging {

  implicit val materializer: Materializer = ActorMaterializer()

  private val MaximumFrameLength: Int = 1024 * 1024 // 1 MB

  import system.dispatcher

  private val hostUri = if (host.startsWith("http:")) {
    Uri(host)
  } else {
    Uri("http://" + host).withPort(8123)
  }

  private val Headers = {
    import HttpEncodingRange.apply
    import akka.http.scaladsl.model.headers.HttpEncodings.{deflate, gzip}
    import akka.http.scaladsl.model.headers.`Accept-Encoding`
    immutable.Seq(`Accept-Encoding`(gzip, deflate))
  }
  private var enableHttpCompression = false

  private def enableHttpCompressionParam: (String, String) = "enable_http_compression" -> (if (enableHttpCompression) "1" else "0")

  private def readOnlyParam(readOnly: Boolean): (String, String) = "readonly" -> (if (readOnly) "1" else "0")

  private val pool = Http().superPool[Promise[HttpResponse]]()
  private val queue = Source.queue[(HttpRequest, Promise[HttpResponse])](bufferSize, OverflowStrategy.dropNew)
    .via(pool)
    .toMat(Sink.foreach {
      case ((Success(resp), p)) => p.success(resp)
      case ((Failure(e), p)) => p.failure(e)
    })(Keep.left)
    .run

  logger.info(s"Starting Clickhouse Client connecting to $hostUri, database $database")
  validateConnection()

  private def validateConnection(): Unit = {
    Await.ready(query("SELECT 1"), 10.seconds).onFailure {
      case _: Throwable => logger.error(s"Server is unavailable! Couldn't connect to $hostUri")
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
    val request = toHttpRequest(sql, readOnly = true)
    executeRequest(request, sql, retries = 5) // Maximum 5 retries
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
    val request = toHttpRequest(sql, readOnly = false)
    executeRequest(request, sql)
  }

  def execute(sql: String, entity: String): Future[String] = {
    val request = toHttpRequest(sql, readOnly = false, Option(entity))
    executeRequest(request, sql)
  }

  /**
    * Creates a stream of the SQL query that will delimit the result from Clickhouse on new-line
    *
    * @param sql a valid Clickhouse SQL string
    */
  def source(sql: String): Source[String, NotUsed] = {
    Source.fromFuture(singleRequest(toHttpRequest(sql)))
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
    val request = toHttpRequest(sql, readOnly = false, Some(entity))
    executeRequest(request, sql)
  }

  private def executeRequest(request: HttpRequest, sql: String, retries: Int = 0): Future[String] = {
    handleResponse(singleRequest(request), sql).recoverWith {
      // The http server closed the connection unexpectedly before delivering responses for 1 outstanding requests
      case e: RuntimeException if e.getMessage.contains("The http server closed the connection unexpectedly") && retries > 0 =>
        logger.warn(s"Unexpected connection closure, retries left: $retries", e)
        //Retry the request with 1 less retry
        executeRequest(request, sql, retries - 1)
      case e: Throwable =>
        Future.failed(new ClickhouseException(e.getMessage, sql, e))
    }
  }

  private def singleRequest(request: HttpRequest): Future[HttpResponse] = {
    val promise = Promise[HttpResponse]

    queue.offer(request -> promise).flatMap {
      case QueueOfferResult.Enqueued =>
        promise.future
      case QueueOfferResult.Dropped =>
        Future.failed(new RuntimeException(s"Queue is full"))
      case QueueOfferResult.QueueClosed =>
        Future.failed(new RuntimeException(s"Queue is closed"))
      case QueueOfferResult.Failure(e) =>
        Future.failed(e)
    }
  }

  private def toHttpRequest(query: String, readOnly: Boolean = true, entity: Option[RequestEntity] = None) = {
    entity match {
      case Some(e) =>
        logger.debug(s"Executing clickhouse query [$query] with entity payload of length ${e.contentLengthOption}")
        HttpRequest(method = HttpMethods.POST, uri =
          hostUri.withQuery(Query("query" -> query, enableHttpCompressionParam)), entity = e, headers = Headers)
      case None =>
        logger.debug(s"Executing clickhouse query [$query]")
        HttpRequest(method = HttpMethods.POST, uri =
          hostUri.withQuery(Query(readOnlyParam(readOnly), enableHttpCompressionParam)), entity = query, headers = Headers)
    }
  }

  private def handleResponse(responseFuture: Future[HttpResponse], query: String): Future[String] = {
    responseFuture.flatMap { response =>
      val encoding = response.encoding
      response match {
        case HttpResponse(StatusCodes.OK, _, entity, _) =>
          entityToString(entity, encoding)
        case HttpResponse(code, _, entity, _) =>
          entityToString(entity, encoding).flatMap(response =>
            Future.failed(new ClickhouseException(s"Server returned code $code; $response", query))
          )
      }
    }
  }

  private def entityToString(entity: ResponseEntity, encoding: HttpEncoding): Future[String] = {
    entity.dataBytes.runFold(ByteString(""))(_ ++ _).flatMap { byteString =>
      encoding match {
        case HttpEncodings.gzip => Gzip.decode(byteString)
        case _ => Future.successful(byteString)
      }
    }.map(_.utf8String)
  }

}
