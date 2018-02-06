package com.crobox.clickhouse.testkit

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.{Done, NotUsed}
import com.crobox.clickhouse.ClickhouseClient
import com.crobox.clickhouse.stream.ClickhouseBulkActor.Insert
import com.crobox.clickhouse.stream.{ClickhouseIndexingSubscriber, SubscriberConfig}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Random, Try}

trait ClickhouseSpec extends SuiteMixin with BeforeAndAfter with BeforeAndAfterAll {
  this: Suite =>
  def config: Config = ConfigFactory.load()
  //  actor system needs to be lazy, if wanting to override it please override this method
  def buildClickHouseSystem(): ActorSystem = ActorSystem("clickhouse-test")

  private lazy implicit val system: ActorSystem        = buildClickHouseSystem()
  private lazy implicit val materializer: Materializer = ActorMaterializer()
  import system.dispatcher

  def clickClient: ClickhouseClient = internalClient

  protected val database                            = s"crobox_clickhouse_client_${Random.nextInt(1000000)}"
  private lazy val internalClient: ClickhouseClient = new ClickhouseClient(config, database)

  private def sql(query: String): String = {
    val result = if (query.startsWith("SHOW") || query.startsWith("SELECT")) {
      internalClient.query(query)
    } else {
      internalClient.execute(query)
    }
    Await
      .result(result.recoverWith {
        case e: Throwable => Future.successful(e.getMessage)
      }, 10.seconds)
      .trim()
  }

  def dropAllTables(): Unit = {

    // Drop all tables
    val tables = sql(s"SHOW TABLES FROM $database").split("\n")
    tables.foreach(t => blockUntilTableDropped(clickClient.table(t)))
  }

  def blockUntilTableDropped(table: String): Unit = {
    sql(s"DROP TABLE IF EXISTS $table")
    blockUntilTableIsMissing(table)
  }

  def blockUntilTableIsMissing(table: String): Unit =
    blockUntil(s"Expected that table $table is missing") { () =>
      sql(s"SELECT 1 FROM $table").contains("DB::Exception")
    }

  def blockUntilTableExists(table: String): Unit =
    blockUntil(s"Expected that table $table exists") { () =>
      sql(s"SELECT 1 FROM $table") == ""
    }

  def blockUntilRowsInTable(rowCount: Int, tableName: String): Unit =
    blockUntil(s"Expected to find $rowCount in table $tableName") { () =>
      Try(sql(s"SELECT COUNT(*) FROM $tableName").toInt).getOrElse(-1) >= rowCount
    }

  def blockUntilExactRowsInTable(rowCount: Int, tableName: String): Unit =
    blockUntil(s"Expected to find $rowCount in table $tableName") { () =>
      sql(s"SELECT COUNT(*) FROM $tableName") == rowCount.toString
    }

  /* Borrowed from Elastic4s ElasticSugar */
  def blockUntil(explain: String)(predicate: () => Boolean): Unit = {

    var backoff = 0
    var done    = false

    while (backoff <= 16 && !done) {
      if (backoff > 0) Thread.sleep(200 * backoff)
      backoff = backoff + 1
      try {
        done = predicate()
      } catch {
        case _: Throwable =>
      }
    }

    require(done, s"Failed waiting on: $explain")
  }

  protected def constructFlow[T](iter: Iterable[T], tableName: String, flow: Flow[T, Insert, NotUsed]): Future[Done] = {
    val sinkPromise = Promise[Done]
    val sink = Sink.fromSubscriber(
      new ClickhouseIndexingSubscriber(clickClient,
                                       SubscriberConfig(
                                         completionFn = () => {
                                           blockUntilRowsInTable(iter.size, tableName)
                                           sinkPromise.success(Done)
                                         }
                                       ))
    )
    Source
      .fromIterator(() => iter.toIterator)
      .via(flow)
      .runWith(sink)
    sinkPromise.future
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll() // To be stackable, must call super.beforeAll
    sql(s"CREATE DATABASE IF NOT EXISTS $database")
  }

  override protected def afterAll(): Unit =
    try super.afterAll() // To be stackable, must call super.afterAll
    finally sql(s"DROP DATABASE IF EXISTS $database")
}
