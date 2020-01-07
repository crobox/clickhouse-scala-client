package com.crobox.clickhouse

import com.crobox.clickhouse.dsl.TestSchema
import com.crobox.clickhouse.dsl.execution.ClickhouseQueryExecutor
import com.crobox.clickhouse.dsl.schemabuilder.{CreateTable, Engine}
import com.crobox.clickhouse.testkit.ClickhouseSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.ExecutionContext

trait TestSchemaClickhouseQuerySpec extends ClickhouseSpec with BeforeAndAfterAll with TestSchema with ScalaFutures {
  this: Suite =>
  val table1Entries: Seq[Table1Entry] = Seq()
  val table2Entries: Seq[Table2Entry] = Seq()

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
    } yield {}
    whenReady(tables) { _ =>
      val inserts = for {
        _ <- table1Entries.into(OneTestTable)
        _ <- table2Entries.into(TwoTestTable)
      } yield {}
      inserts.futureValue
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}
