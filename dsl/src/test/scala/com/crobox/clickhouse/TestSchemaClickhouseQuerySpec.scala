package com.crobox.clickhouse

import com.crobox.clickhouse.dsl.TestSchema
import com.crobox.clickhouse.dsl.execution.ClickhouseQueryExecutor
import com.crobox.clickhouse.dsl.schemabuilder.{CreateTable, Engine}
import com.crobox.clickhouse.testkit.ClickhouseSpec
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.scalatest.concurrent.ScalaFutures

trait TestSchemaClickhouseQuerySpec extends ClickhouseSpec with BeforeAndAfterAll with TestSchema with ScalaFutures {
  this: Suite =>
  val table1Entries: Seq[Table1Entry] = Seq()
  val table2Entries: Seq[Table2Entry] = Seq()

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit lazy val chExecuter: ClickhouseQueryExecutor = ClickhouseQueryExecutor.default(clickClient)
  override val config: Config                      = ConfigFactory.load()

  override def beforeAll(): Unit = {
    super.beforeAll()
    val tables = for {
      _ <- clickClient.execute(
        CreateTable(OneTestTable, Engine.Memory, ifNotExists = true, clickClient.database).query
      )
      _ <- clickClient.execute(
        CreateTable(
          TwoTestTable,
          Engine.Memory,
          ifNotExists = true,
          clickClient.database
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

}
