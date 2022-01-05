package com.crobox.clickhouse

import com.crobox.clickhouse.dsl.execution.ClickhouseQueryExecutor
import com.crobox.clickhouse.dsl.language.ClickhouseTokenizerModule
import com.crobox.clickhouse.dsl.schemabuilder.{CreateTable, Engine}
import com.crobox.clickhouse.dsl.{Query, ValueInsertion}
import com.crobox.clickhouse.internal.QuerySettings
import com.crobox.clickhouse.testkit.ClickhouseSpec
import org.scalatest.Suite
import org.scalatest.time.{Millis, Seconds, Span}
import spray.json.DefaultJsonProtocol.{jsonFormat, StringJsonFormat}
import spray.json.RootJsonFormat

import scala.concurrent.{ExecutionContext, Future}

trait DslITSpec extends ClickhouseClientSpec with ClickhouseSpec with TestSchema {
  this: Suite =>
  val table1Entries: Seq[Table1Entry] = Seq()
  val table2Entries: Seq[Table2Entry] = Seq()
  val table3Entries: Seq[Table2Entry] = Seq()

  implicit val ec: ExecutionContext

  implicit lazy val chExecutor: ClickhouseQueryExecutor = ClickhouseQueryExecutor.default(clickClient)
  val clickhouseTokenizer: ClickhouseTokenizerModule    = new ClickhouseTokenizerModule {}

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(20, Millis)))

  def execute(query: Query): Future[String] = {
    implicit val settings: QuerySettings = QuerySettings()
    clickClient.query(clickhouseTokenizer.toSql(query.internalQuery, None)).map(_.trim)
  }

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

  case class StringResult(result: String)
  implicit val stringResultFormat: RootJsonFormat[StringResult] =
    jsonFormat[String, StringResult](StringResult.apply, "result")
}
