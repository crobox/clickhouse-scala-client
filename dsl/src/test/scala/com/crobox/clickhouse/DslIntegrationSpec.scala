package com.crobox.clickhouse

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.execution.ClickhouseQueryExecutor
import com.crobox.clickhouse.dsl.language.{ClickhouseTokenizerModule, TokenizeContext}
import com.crobox.clickhouse.dsl.schemabuilder.{CreateTable, Engine}
import com.crobox.clickhouse.testkit.ClickhouseSpec
import org.scalatest.Suite

import scala.concurrent.{ExecutionContext, Future}

trait DslIntegrationSpec
    extends ClickhouseClientSpec
    with ClickhouseSpec
    with TestSchema
    with ClickhouseTokenizerModule {
  this: Suite =>

  implicit def ctx: TokenizeContext = TokenizeContext(clickClient.getServerVersion)

  protected def r(query: Column): String =
    runSql(select(query)).futureValue.trim

  protected def runSql(query: OperationalQuery): Future[String] =
    clickClient.query(toSql(query.internalQuery, None))

  val table1Entries: Seq[Table1Entry] = Seq()
  val table2Entries: Seq[Table2Entry] = Seq()
  val table3Entries: Seq[Table3Entry] = Seq()

  implicit val ec: ExecutionContext

  implicit lazy val chExecutor: ClickhouseQueryExecutor = ClickhouseQueryExecutor.default(clickClient)

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
