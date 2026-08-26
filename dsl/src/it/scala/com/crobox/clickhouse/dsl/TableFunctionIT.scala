package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.{dsl, DslITSpec}

/**
 * Table functions executed rather than only rendered, so an argument order that parses either way is still caught.
 *
 * Qualified as `dsl.numbers` / `dsl.all`: an inherited member shadows an imported one, and this spec inherits both a
 * `numbers` column from the test schema and ScalaTest's `all` matcher.
 */
class TableFunctionIT extends DslITSpec {

  private def longs(query: OperationalQuery, column: String): Seq[Long] =
    queryExecutor.executeRows(query).futureValue.rows.map(_.requiredByName[Long](column))

  "numbers" should "count up from zero" in {
    longs(select(dsl.all) from dsl.numbers(3), "number") shouldBe Seq(0, 1, 2)
  }

  it should "count up from the offset when given one" in {
    longs(select(dsl.all) from dsl.numbers(10, 3), "number") shouldBe Seq(10, 11, 12)
  }

  "zeros" should "produce the row count without a counter" in {
    val rows = queryExecutor.executeRows(select(dsl.all) from dsl.zeros(3)).futureValue.rows
    rows should have size 3
    rows.head.names shouldBe Seq("zero")
  }

  "values" should "build an inline table, in row order" in {
    val rows = queryExecutor
      .executeRows(select(dsl.all) from dsl.values(tuple(const(1), const("x")), tuple(const(2), const("y"))))
      .futureValue
      .rows
    rows.map(_.requiredByName[String]("c2")) shouldBe Seq("x", "y")
  }

  it should "name and type the columns when given a structure" in {
    val rows = queryExecutor
      .executeRows(select(dsl.all) from valuesWithStructure("a UInt32", const(7), const(8)))
      .futureValue
      .rows
    rows.map(_.requiredByName[Int]("a")) shouldBe Seq(7, 8)
  }

  "A table function" should "work under an alias and as a join right-hand side" in {
    longs(select(ref[Long]("number")) from (dsl.numbers(2), "n"), "number") shouldBe Seq(0, 1)

    // CROSS JOIN against two generated rows doubles the left side.
    val crossed = select(shieldId).from(OneTestTable).join(JoinQuery.CrossJoin, dsl.numbers(2))
    queryExecutor.executeRows(crossed).futureValue.rows should have size (table1Entries.size * 2)
  }

  override val table1Entries: Seq[Table1Entry] = Seq(Table1Entry(java.util.UUID.randomUUID()))
}
