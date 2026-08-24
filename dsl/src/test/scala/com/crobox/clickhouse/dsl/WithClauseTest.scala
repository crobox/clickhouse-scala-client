package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslTestSpec

/**
 * `WITH` rendering, for all three of the forms ClickHouse puts behind the keyword. The two expression forms read
 * `<value> AS <name>` and the CTE form reads `<name> AS (<query>)`, so getting one confused for the other produces
 * something that still looks like a WITH clause.
 */
class WithClauseTest extends DslTestSpec {

  private def sql(query: OperationalQuery): String = toSql(query.internalQuery, None)

  "WITH" should "name a plain expression" in {
    val query = select(ref[Int]("one")).from(OneTestTable).withCte(WithExpression(const(1), "one"))
    sql(query) should matchSQL(s"WITH 1 AS one SELECT one FROM ${OneTestTable.quoted}")
  }

  it should "come before SELECT" in {
    val query = select(shieldId).from(OneTestTable).where(shieldId isEq "a").withCte(WithExpression(const(1), "one"))
    sql(query) should matchSQL(
      s"WITH 1 AS one SELECT shield_id FROM ${OneTestTable.quoted} WHERE shield_id = 'a'"
    )
  }

  // The shape #142 asked for.
  it should "name the value a scalar subquery returns" in {
    val total = WithScalarQuery(select(count()).from(TwoTestTable), "total")
    val query = select(itemId, ref[Long]("total")).from(TwoTestTable).withCte(total)
    sql(query) should matchSQL(
      s"WITH (SELECT count() FROM ${TwoTestTable.quoted}) AS total " +
        s"SELECT item_id, total FROM ${TwoTestTable.quoted}"
    )
  }

  it should "declare a CTE with the name first, unlike the expression forms" in {
    val recent = WithTable("recent", select(itemId).from(TwoTestTable))
    sql(select(itemId).from(recent).withCte(recent)) should matchSQL(
      s"WITH recent AS (SELECT item_id FROM ${TwoTestTable.quoted}) SELECT item_id FROM recent"
    )
  }

  it should "reference a CTE as a bare identifier, with no database to qualify it" in {
    WithTable("recent", select(itemId).from(TwoTestTable)).quoted shouldBe "recent"
  }

  it should "render several entries in the order given" in {
    val query = select(ref[Int]("one"))
      .from(OneTestTable)
      .withCte(WithExpression(const(1), "one"), WithExpression(const(2), "two"))
    sql(query) should matchSQL(s"WITH 1 AS one, 2 AS two SELECT one FROM ${OneTestTable.quoted}")
  }

  it should "accumulate across calls rather than replacing" in {
    val query = select(ref[Int]("one"))
      .from(OneTestTable)
      .withCte(WithExpression(const(1), "one"))
      .withCte(WithExpression(const(2), "two"))
    sql(query) should matchSQL(s"WITH 1 AS one, 2 AS two SELECT one FROM ${OneTestTable.quoted}")
  }

  it should "mix the forms in one clause" in {
    val query = select(itemId)
      .from(TwoTestTable)
      .withCte(
        WithExpression(const(1), "one"),
        WithScalarQuery(select(count()).from(TwoTestTable), "total"),
        WithTable("recent", select(itemId).from(ThreeTestTable))
      )
    sql(query) should matchSQL(
      s"WITH 1 AS one, (SELECT count() FROM ${TwoTestTable.quoted}) AS total, " +
        s"recent AS (SELECT item_id FROM ${ThreeTestTable.quoted}) SELECT item_id FROM ${TwoTestTable.quoted}"
    )
  }

  // quoteIdentifier quotes on syntax, not on reserved words -- `WITH 1 AS select` is accepted by the server as-is.
  it should "quote a name that could not be parsed bare" in {
    val query = select(shieldId).from(OneTestTable).withCte(WithExpression(const(1), "my name"))
    sql(query) should matchSQL(s"WITH 1 AS `my name` SELECT shield_id FROM ${OneTestTable.quoted}")
  }

  it should "render nothing when there are no entries" in {
    sql(select(shieldId).from(OneTestTable).withCte()) should matchSQL(
      s"SELECT shield_id FROM ${OneTestTable.quoted}"
    )
  }

  // Entries are scoped to the SELECT that declares them, so an inner query keeps its own rather than hoisting.
  it should "stay inside the subquery that declared it" in {
    val inner = select(shieldId).from(OneTestTable).withCte(WithExpression(const(1), "one"))
    sql(select(shieldId).from(inner)) should matchSQL(
      s"SELECT shield_id FROM (WITH 1 AS one SELECT shield_id FROM ${OneTestTable.quoted})"
    )
  }

  it should "stay inside its own UNION ALL branch" in {
    val query = select(shieldId)
      .from(OneTestTable)
      .withCte(WithExpression(const(1), "one"))
      .unionAll(select(shieldId).from(OneTestTable).withCte(WithExpression(const(2), "two")))
    sql(query) should matchSQL(
      s"WITH 1 AS one SELECT shield_id FROM ${OneTestTable.quoted} " +
        s"UNION ALL WITH 2 AS two SELECT shield_id FROM ${OneTestTable.quoted}"
    )
  }
}
