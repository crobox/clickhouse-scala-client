package com.crobox.clickhouse.testkit

import com.crobox.clickhouse.ClickhouseClient
import com.typesafe.config.Config
import org.scalatest._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Random, Try}

trait ClickhouseSpec extends SuiteMixin with BeforeAndAfter with BeforeAndAfterAll with ClickhouseMatchers {
  this: Suite =>

  val config: Config

  val clickhouseSpecTimeout: FiniteDuration = 10.seconds
  lazy val database                         = s"crobox_clickhouse_client_${Random.nextInt(1000000)}"

  /** Explicitly add this sequence that can be overwritten in order to create multiple databases for a test */
  lazy val databases = Seq(database)

  /* When true, databases will be dropped when finished (e.g. AfterAll). Can be overwritten to disable */
  val dropDatabasesAfterTest = true

  def clickClient: ClickhouseClient = internalClient

  private lazy val internalClient: ClickhouseClient = new ClickhouseClient(Some(config))

  protected def sql(query: String): String = {
    val result = if (query.startsWith("SHOW") || query.startsWith("SELECT")) {
      internalClient.query(query)
    } else {
      internalClient.execute(query)
    }
    Await
      .result(result.recoverWith {
        case e: Throwable => Future.successful(e.getMessage)
      }(ExecutionContext.Implicits.global), clickhouseSpecTimeout)
      .trim()
  }

  def dropAllTables(db: String = database): Int = {
    val rawTables = sql(s"SHOW TABLES FROM $db")
    if (rawTables.isEmpty) {
      0
    } else {
      val tables = rawTables.split("\n")
      // Drop all tables
      tables.foreach(t => blockUntilTableDropped(s"$db.$t"))
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
    databases.foreach(db => sql(s"CREATE DATABASE IF NOT EXISTS $db"))
  }

  override protected def afterAll(): Unit =
    try super.afterAll() // To be stackable, must call super.afterAll
    finally {
      if (dropDatabasesAfterTest) {
        databases.foreach(db => sql(s"DROP DATABASE IF EXISTS $db"))
      }
      Await.result(internalClient.shutdown(), clickhouseSpecTimeout)
    }

  // Returns the Clickhouse Version. DEFAUlT VALUE *must* equal the one set in .travis.yml AND docker-compose.xml
  lazy val ClickHouseVersion: String   = clickClient.getServerVersion
  private lazy val chVersion: Seq[Int] = ClickHouseVersion.split('.').toSeq.map(_.filter(_.isDigit)).map(_.toInt)

  def assumeMinimalClickhouseVersion(version: Int): Assertion =
    assume(chVersion(0) >= version, s"ClickhouseVersion: $ClickHouseVersion >= $version does NOT hold")

  def assumeMinimalClickhouseVersion(version: Int, subVersion: Int): Assertion =
    assume(chVersion(0) >= version && chVersion(1) >= subVersion,
           s"ClickhouseVersion: $ClickHouseVersion >= $version.$subVersion does NOT hold")

  def mustMatchClickHouseVersion(version: Int, testFun: => Any): Any =
    if (chVersion(0) >= version) {
      // continue with test
      testFun
    } else {
      // abort test
      cancel()
    }
}
