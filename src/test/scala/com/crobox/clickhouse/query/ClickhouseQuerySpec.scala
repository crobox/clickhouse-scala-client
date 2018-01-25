package com.crobox.clickhouse.query

import com.crobox.clickhouse.query.clickhouse.{ClickhouseQueryExecutor, ClickhouseTokenizerModule}
import com.crobox.clickhouse.query.schemabuilder.{Column, ColumnType, CreateTable, Engine}
import com.crobox.clickhouse.test.{ClickhouseSpec, TestSchema}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import spray.json._

trait ClickhouseQuerySpec extends ClickhouseSpec with BeforeAndAfterAll with TestSchema with ScalaFutures {

  val table1Entries: Seq[Table1Entry] = Seq()
  val table2Entries: Seq[Table2Entry] = Seq()

  val chExecuter = new ClickhouseQueryExecutor with ClickhouseTokenizerModule {}
  import scala.concurrent.ExecutionContext.Implicits.global

  override def beforeAll(): Unit = {
    super.beforeAll()
    val tables = for {
      _ <- clickClient.execute(
        CreateTable(OneTestTable.name,
                    Seq(Column(shieldId.name), Column(timestampColumn.name, ColumnType.Long)),
                    Engine.Memory,
                    ifNotExists = true,
                    clickClient.database).query
      )
      _ <- clickClient.execute(
        CreateTable(
          TwoTestTable.name,
          Seq(Column(itemId.name),
              Column(col1.name),
              Column(col2.name, ColumnType.Int),
              Column(col3.name),
              Column(col4.name)),
          Engine.Memory,
          ifNotExists = true,
          clickClient.database
        ).query
      )
    } yield {}
    whenReady(tables) { _ =>
      val entries1 = table1Entries.map(entry => entry.toJson.compactPrint).mkString("\n") + "\n"
      val entries2 = table2Entries.map(entry => entry.toJson.compactPrint).mkString("\n") + "\n"
      val inserts = for {
        _ <- clickClient.execute(s"INSERT INTO ${clickClient.table(OneTestTable.name)} FORMAT JSONEachRow", entries1)
        _ <- clickClient.execute(s"INSERT INTO ${clickClient.table(TwoTestTable.name)} FORMAT JSONEachRow", entries2)
      } yield {}
      inserts.futureValue
    }
  }

}
