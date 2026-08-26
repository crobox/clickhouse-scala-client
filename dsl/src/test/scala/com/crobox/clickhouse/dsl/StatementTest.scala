package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslTestSpec

/** Rendering of the data-changing statements. */
class StatementTest extends DslTestSpec {

  private def sql(statement: Statement): String = toStatementSql(statement)

  private val condition = (col2 isEq 1) and (col3 isEq "x")

  "DELETE FROM" should "render the lightweight delete" in {
    sql(DeleteFrom(TwoTestTable, condition)) should matchSQL(
      s"DELETE FROM ${TwoTestTable.quoted} WHERE column_2 = 1 AND column_3 = 'x'"
    )
  }

  it should "take ON CLUSTER before the WHERE" in {
    sql(DeleteFrom(TwoTestTable, condition, Option("my cluster"))) should matchSQL(
      s"DELETE FROM ${TwoTestTable.quoted} ON CLUSTER `my cluster` WHERE column_2 = 1 AND column_3 = 'x'"
    )
  }

  "ALTER TABLE ... DELETE" should "render the mutation form" in {
    sql(AlterDelete(TwoTestTable, condition)) should matchSQL(
      s"ALTER TABLE ${TwoTestTable.quoted} DELETE WHERE column_2 = 1 AND column_3 = 'x'"
    )
  }

  "ALTER TABLE ... UPDATE" should "render one assignment" in {
    sql(AlterUpdate(TwoTestTable, Seq(Assignment(col3, const("y"))), col2 isEq 1)) should matchSQL(
      s"ALTER TABLE ${TwoTestTable.quoted} UPDATE column_3 = 'y' WHERE column_2 = 1"
    )
  }

  it should "render several assignments, and an expression on the right" in {
    val statement = AlterUpdate(
      TwoTestTable,
      Seq(Assignment(col3, const("y")), Assignment(col4, concat(col3, const("!")))),
      col2 isEq 1,
      Option("c")
    )
    sql(statement) should matchSQL(
      s"ALTER TABLE ${TwoTestTable.quoted} ON CLUSTER c " +
        s"UPDATE column_3 = 'y', column_4 = concat(column_3, '!') WHERE column_2 = 1"
    )
  }

  it should "refuse an empty assignment list" in {
    an[IllegalArgumentException] should be thrownBy AlterUpdate(TwoTestTable, Seq.empty, col2 isEq 1)
  }

  "DROP PARTITION" should "render a partition id as a quoted literal" in {
    sql(DropPartition(TwoTestTable, PartitionRef.Id("202601"))) should matchSQL(
      s"ALTER TABLE ${TwoTestTable.quoted} DROP PARTITION ID '202601'"
    )
  }

  it should "escape a partition id rather than splice it" in {
    sql(DropPartition(TwoTestTable, PartitionRef.Id("it's"))) should include("""ID 'it\'s'""")
  }

  it should "render a partitioning expression unquoted" in {
    sql(DropPartition(TwoTestTable, PartitionRef.Expression(const(202601)))) should matchSQL(
      s"ALTER TABLE ${TwoTestTable.quoted} DROP PARTITION 202601"
    )
  }

  it should "take ON CLUSTER" in {
    sql(DropPartition(TwoTestTable, PartitionRef.Id("202601"), Option("c"))) should matchSQL(
      s"ALTER TABLE ${TwoTestTable.quoted} ON CLUSTER c DROP PARTITION ID '202601'"
    )
  }
}
