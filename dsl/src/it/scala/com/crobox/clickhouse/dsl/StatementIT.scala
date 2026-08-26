package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl.schemabuilder.{ColumnType, CreateTable, Engine}
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

/**
 * The data-changing statements, executed. Needs its own table: the shared fixtures are Engine.Memory, which supports
 * neither lightweight deletes nor mutations, and a DROP PARTITION needs something partitioned to drop.
 */
class StatementIT extends DslITSpec {

  private object MutableTable extends Table {
    override val database             = StatementIT.this.database
    override val name                 = "mutableTestTable"
    val part: NativeColumn[Int]       = NativeColumn("part", ColumnType.UInt32)
    val key: NativeColumn[Int]        = NativeColumn("key", ColumnType.UInt32)
    val payload: NativeColumn[String] = NativeColumn("payload")
    override val columns              = List(part, key, payload)
  }

  private case class Entry(part: Int, key: Int, payload: String)
  private implicit val entryFormat: RootJsonFormat[Entry] = jsonFormat3(Entry.apply)

  private val entries = Seq(Entry(1, 1, "a"), Entry(1, 2, "b"), Entry(2, 3, "c"))

  private def reset(): Unit = {
    clickClient
      .execute(
        CreateTable(
          MutableTable,
          Engine.MergeTree(partition = Seq(MutableTable.part.name), primaryKey = Seq(MutableTable.key)),
          ifNotExists = true
        ).query
      )
      .futureValue
    clickClient.execute(s"TRUNCATE TABLE ${MutableTable.quoted}").futureValue
    queryExecutor.insert(MutableTable, entries).futureValue
  }

  /**
   * Mutations return once queued, so every statement here runs with mutations_sync so the assertion sees the result.
   */
  private implicit val synchronous: com.crobox.clickhouse.internal.QuerySettings =
    com.crobox.clickhouse.internal.QuerySettings(settings = Map("mutations_sync" -> "2"))

  private def payloads: Seq[String] =
    queryExecutor
      .executeRows(select(MutableTable.payload) from MutableTable orderBy MutableTable.key)
      .futureValue
      .rows
      .map(_.requiredByName[String]("payload"))

  private def keys: Seq[Int] =
    queryExecutor
      .executeRows(select(MutableTable.key) from MutableTable orderBy MutableTable.key)
      .futureValue
      .rows
      .map(_.requiredByName[Int]("key"))

  "DELETE FROM" should "remove exactly the rows the condition matches" in {
    reset()
    queryExecutor.executeStatement(DeleteFrom(MutableTable, MutableTable.key isEq 2)).futureValue
    keys shouldBe Seq(1, 3)
  }

  "ALTER TABLE ... DELETE" should "remove the rows the condition matches" in {
    reset()
    queryExecutor.executeStatement(AlterDelete(MutableTable, MutableTable.key > 1)).futureValue
    keys shouldBe Seq(1)
  }

  "ALTER TABLE ... UPDATE" should "assign to the rows the condition matches and no others" in {
    reset()
    queryExecutor
      .executeStatement(
        AlterUpdate(MutableTable, Seq(Assignment(MutableTable.payload, const("z"))), MutableTable.key isEq 2)
      )
      .futureValue
    payloads shouldBe Seq("a", "z", "c")
  }

  it should "evaluate an expression on the right-hand side" in {
    reset()
    queryExecutor
      .executeStatement(
        AlterUpdate(
          MutableTable,
          Seq(Assignment(MutableTable.payload, concat(MutableTable.payload, const("!")))),
          MutableTable.part isEq 1
        )
      )
      .futureValue
    payloads shouldBe Seq("a!", "b!", "c")
  }

  "DROP PARTITION" should "discard a partition addressed by id" in {
    reset()
    queryExecutor.executeStatement(DropPartition(MutableTable, PartitionRef.Id("1"))).futureValue
    keys shouldBe Seq(3)
  }

  it should "discard a partition addressed by the partitioning expression" in {
    reset()
    queryExecutor.executeStatement(DropPartition(MutableTable, PartitionRef.Expression(const(2)))).futureValue
    keys shouldBe Seq(1, 2)
  }

  // The whole point of taking a DSL condition rather than a string: nothing has to splice a rendered fragment.
  "A statement's WHERE" should "escape a value rather than splice it" in {
    reset()
    queryExecutor
      .executeStatement(DeleteFrom(MutableTable, MutableTable.payload isEq "' OR 1 = 1 --"))
      .futureValue
    keys shouldBe Seq(1, 2, 3)
  }
}
