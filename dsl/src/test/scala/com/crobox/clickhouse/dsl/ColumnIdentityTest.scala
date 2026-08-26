package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslTestSpec

class ColumnIdentityTest extends DslTestSpec {

  private def sql(query: OperationalQuery): String = toSql(query.internalQuery, None)

  "Select-list de-duplication" should "keep two different expressions over the same column" in {
    SelectQuery(Seq(sum(col2))).addColumn(uniq(col2)).columns should have size 2
  }

  it should "still drop a column already present" in {
    SelectQuery(Seq(col2)).addColumn(col2).columns should have size 1
  }

  it should "still drop an identical expression" in {
    SelectQuery(Seq(sum(col2))).addColumn(sum(col2)).columns should have size 1
  }

  it should "not treat an expression as the column it wraps" in {
    SelectQuery(Seq(sum(col2))).addColumn(col2).columns should have size 2
  }

  it should "remove the column given, not everything sharing its name" in {
    SelectQuery(Seq(col2, sum(col2))).removeColumn(col2).columns should contain theSameElementsAs Seq(sum(col2))
  }

  // The damaging case: the grouping column was left out of the projection, so the sums came back with nothing
  // identifying which group each belonged to.
  "groupBy" should "project the grouping column even when an aggregate over it is selected" in {
    sql(select(sum(col2)).from(TwoTestTable).groupBy(col2)) should matchSQL(
      s"SELECT sum(column_2), column_2 FROM ${TwoTestTable.quoted} GROUP BY column_2"
    )
  }

  it should "not project the same grouping column twice" in {
    sql(select(col2).from(TwoTestTable).groupBy(col2)) should matchSQL(
      s"SELECT column_2 FROM ${TwoTestTable.quoted} GROUP BY column_2"
    )
  }

  "orderBy" should "project an expression that differs from what is already selected" in {
    sql(select(sum(col2)).from(TwoTestTable).orderBy(uniq(col2))) should matchSQL(
      s"SELECT sum(column_2), uniq(column_2) FROM ${TwoTestTable.quoted} ORDER BY uniq(column_2) ASC"
    )
  }

  "Re-aliasing" should "replace the alias rather than emit two" in {
    sql(select((shieldId as "from_pv") as "from_start")) should matchSQL("SELECT shield_id AS from_start")
  }

  it should "replace through aliased" in {
    sql(select((shieldId as "from_pv").aliased("from_start"))) should matchSQL("SELECT shield_id AS from_start")
  }

  it should "replace through the column-taking overload" in {
    sql(select((shieldId as "from_pv") as itemId)) should matchSQL("SELECT shield_id AS item_id")
  }

  it should "keep the original rather than nesting" in {
    (shieldId as "from_pv") as "from_start" shouldBe AliasedColumn(shieldId, "from_start")
  }

  it should "survive being aliased repeatedly" in {
    sql(select(shieldId as "a" as "b" as "c")) should matchSQL("SELECT shield_id AS c")
  }

  // Ordering by an alias must not append a second copy of it to the projection. By-value de-duplication alone did,
  // which widened every row of every query that orders by an aggregate's alias.
  "Ordering by an alias" should "not duplicate the aliased column" in {
    sql(select(uniq(col2) as "value").from(TwoTestTable).orderBy(ref[Long]("value"))) should matchSQL(
      s"SELECT uniq(column_2) AS value FROM ${TwoTestTable.quoted} ORDER BY value ASC"
    )
  }

  it should "not duplicate it when grouping either" in {
    sql(select(col3 as "value").from(TwoTestTable).groupBy(ref[String]("value"))) should matchSQL(
      s"SELECT column_3 AS value FROM ${TwoTestTable.quoted} GROUP BY value"
    )
  }

  it should "still project a grouping column the projection does not produce" in {
    sql(select(col3 as "value").from(TwoTestTable).groupBy(col2)) should matchSQL(
      s"SELECT column_3 AS value, column_2 FROM ${TwoTestTable.quoted} GROUP BY column_2"
    )
  }

  // A stored column and an alias of the same name are the same identifier to the server, which resolves the alias.
  it should "treat a native column of the alias name as already projected" in {
    sql(select(col3 as "column_2").from(TwoTestTable).orderBy(col2)) should matchSQL(
      s"SELECT column_3 AS column_2 FROM ${TwoTestTable.quoted} ORDER BY column_2 ASC"
    )
  }

  // The documented answer to #24: to refer to an alias from an enclosing query, reference it rather than re-aliasing.
  it should "reference an inner alias from an enclosing query" in {
    val fromPv = shieldId as "from_pv"
    val query  = select(ref[String]("from_pv") as "from_start").from(select(fromPv).from(OneTestTable))
    sql(query) should matchSQL(
      s"SELECT from_pv AS from_start FROM (SELECT shield_id AS from_pv FROM ${OneTestTable.quoted})"
    )
  }
}
