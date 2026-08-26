package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.{dsl, DslTestSpec}

/**
 * Table functions in `FROM`.
 *
 * Qualified as `dsl.numbers` / `dsl.all` throughout: an inherited member shadows an imported one, and this spec
 * inherits both a `numbers` column from the test schema and ScalaTest's `all` matcher. Neither collides for a caller
 * who has not defined their own.
 */
class TableFunctionTest extends DslTestSpec {

  private def sql(query: OperationalQuery): String = toSql(query.internalQuery, None)

  it should "render numbers at both arities" in {
    sql(select(dsl.all).from(dsl.numbers(3))) should matchSQL("SELECT * FROM numbers(3)")
    sql(select(dsl.all).from(dsl.numbers(10, 3))) should matchSQL("SELECT * FROM numbers(10, 3)")
  }

  it should "render zeros" in {
    sql(select(dsl.all).from(dsl.zeros(3))) should matchSQL("SELECT * FROM zeros(3)")
  }

  it should "render values, as rows and with a structure" in {
    sql(select(dsl.all).from(dsl.values(tuple(const(1), const("x")), tuple(const(2), const("y"))))) should matchSQL(
      "SELECT * FROM values((1, 'x'), (2, 'y'))"
    )
    sql(select(dsl.all).from(valuesWithStructure("a UInt32", const(1), const(2)))) should matchSQL(
      "SELECT * FROM values('a UInt32', 1, 2)"
    )
  }

  it should "render merge under its own builder name" in {
    sql(select(dsl.all).from(mergeTables("system", "^one$"))) should matchSQL("SELECT * FROM merge('system', '^one$')")
  }

  it should "render remote and cluster" in {
    sql(select(dsl.all).from(remote("localhost:9000", "system", "one"))) should matchSQL(
      "SELECT * FROM remote('localhost:9000', 'system', 'one')"
    )
    sql(select(dsl.all).from(cluster("default", "system", "one"))) should matchSQL(
      "SELECT * FROM cluster('default', 'system', 'one')"
    )
  }

  it should "render generateRandom at both arities" in {
    sql(select(dsl.all).from(generateRandom("a UInt8"))) should matchSQL("SELECT * FROM generateRandom('a UInt8')")
    sql(select(dsl.all).from(generateRandom("a UInt8", 1, 2, 3))) should matchSQL(
      "SELECT * FROM generateRandom('a UInt8', 1, 2, 3)"
    )
  }

  it should "reach a function that has no builder" in {
    sql(select(dsl.all).from(tableFunction("nulls", const(1)))) should matchSQL("SELECT * FROM nulls(1)")
    sql(select(dsl.all).from(tableFunction("noArgsAtAll"))) should matchSQL("SELECT * FROM noArgsAtAll()")
  }

  it should "take an alias, both ways round" in {
    sql(select(dsl.all).from(dsl.numbers(3), "n")) should matchSQL("SELECT * FROM numbers(3) AS n")
    sql(select(dsl.all).from(dsl.numbers(3)).as("n")) should matchSQL("SELECT * FROM numbers(3) AS n")
  }

  it should "work as a subquery source and as a join right-hand side" in {
    sql(select(dsl.all).from(select(dsl.all).from(dsl.numbers(3)))) should matchSQL(
      "SELECT * FROM (SELECT * FROM numbers(3))"
    )
    sql(select(itemId).from(TwoTestTable).join(JoinQuery.CrossJoin, dsl.numbers(3))) should matchSQL(
      s"SELECT item_id FROM ${TwoTestTable.quoted} AS L1 CROSS JOIN (SELECT * FROM numbers(3)) AS R1"
    )
  }

  // Both read something the table's engine declares, and a function has no engine.
  it should "refuse FINAL and SAMPLE" in {
    an[IllegalArgumentException] should be thrownBy select(dsl.all).from(dsl.numbers(3)).asFinal
    an[IllegalArgumentException] should be thrownBy select(dsl.all).from(dsl.numbers(3)).sample(0.1)
  }
}
