package com.crobox.clickhouse

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.execution.{ClickhouseQueryExecutor, QueryExecutor}
import com.crobox.clickhouse.dsl.language._
import com.crobox.clickhouse.dsl.schemabuilder.{CreateTable, Engine}
import com.crobox.clickhouse.internal.QuerySettings
import com.crobox.clickhouse.testkit.ClickhouseSpec
import org.scalatest.Suite
import org.scalatest.time.{Millis, Seconds, Span}
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import scala.concurrent.Future

trait DslITSpec extends ClickhouseClientSpec with ClickhouseSpec with TestSchema with ClickhouseTokenizerModule {
  this: Suite =>

  implicit lazy val queryExecutor: QueryExecutor = ClickhouseQueryExecutor.default(clickClient)
  implicit def ctx: TokenizeContext              = TokenizeContext(clickClient.serverVersion)

  val table1Entries: Seq[Table1Entry] = Seq()
  val table2Entries: Seq[Table2Entry] = Seq()
  val table3Entries: Seq[Table3Entry] = Seq()

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(20, Millis)))

  def execute(query: Query): Future[String] = {
    implicit val settings: QuerySettings = QuerySettings()
    clickClient.query(toSql(query.internalQuery, None)).map(_.trim)
  }

  protected def r(query: Column): String = runSql(select(query)).futureValue.trim

  protected def runSql(query: OperationalQuery): Future[String] =
    clickClient.query(toSql(query.internalQuery, None))

  override def beforeAll(): Unit = {
    super.beforeAll()
    val tables = for {
      _ <- clickClient.execute(
        CreateTable(OneTestTable, Engine.Memory, ifNotExists = true).query
      )
      _ <- clickClient.execute(
        CreateTable(
          TwoTestTable,
          Engine.Memory,
          ifNotExists = true
        ).query
      )
      _ <- clickClient.execute(
        CreateTable(
          ThreeTestTable,
          Engine.Memory,
          ifNotExists = true
        ).query
      )
    } yield {}
    whenReady(tables) { _ =>
      val inserts = for {
        _ <- table1Entries.into(OneTestTable)
        _ <- table2Entries.into(TwoTestTable)
        _ <- table3Entries.into(ThreeTestTable)
      } yield {}
      inserts.futureValue
    }
  }

  override def afterAll(): Unit = super.afterAll()
}

object DslITSpec {
  case class StringResult(result: String)

  implicit val stringResultFormat: RootJsonFormat[StringResult] =
    jsonFormat[String, StringResult](StringResult.apply, "result")

  case class IntResult(result: Int)

  implicit val intResultFormat: RootJsonFormat[IntResult] =
    jsonFormat[Int, IntResult](IntResult.apply, "result")
}
