package com.crobox.clickhouse.testkit

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import com.crobox.clickhouse.ClickhouseClient
import com.typesafe.config.Config
import org.scalatest._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Random, Try}

trait ClickhouseSpec extends SuiteMixin with BeforeAndAfter with BeforeAndAfterAll {
  this: Suite =>

  val config: Config
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

  def dropAllTables(): Int = {
    val rawTables = sql(s"SHOW TABLES FROM $database")
    if(rawTables.isEmpty) {
      0
    } else {
      val tables = rawTables.split("\n")
      // Drop all tables
      tables.foreach(t => blockUntilTableDropped(clickClient.table(t)))
      tables.size
    }
  }

  def blockUntilTableDropped(table: String): Unit = {
    sql(s"DROP TABLE IF EXISTS $table")
    blockUntilTableIsMissing(table)
  }

  def blockUntilTableIsMissing(table: String): Unit =
    blockUntil(s"Expected that table $table is missing") { () =>
      sql(s"EXISTS TABLE $table") == "0"
    }

  def blockUntilTableExists(table: String): Unit =
    blockUntil(s"Expected that table $table exists") { () =>
      sql(s"EXISTS TABLE $table") == "1"
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
  override protected def beforeAll(): Unit = {
    super.beforeAll() // To be stackable, must call super.beforeAll
    sql(s"CREATE DATABASE IF NOT EXISTS $database")
  }

  override protected def afterAll(): Unit =
    try super.afterAll() // To be stackable, must call super.afterAll
    finally sql(s"DROP DATABASE IF EXISTS $database")
}
