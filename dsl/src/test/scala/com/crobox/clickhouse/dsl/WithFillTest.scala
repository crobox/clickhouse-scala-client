package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslTestSpec

/**
 * `ORDER BY … WITH FILL` and `INTERPOLATE`.
 *
 * `WITH FILL` attaches to a single ORDER BY entry, so the ordering had to stop being a `(Column, OrderingDirection)`
 * tuple. The tests at the bottom pin that the old call shapes still compile through the implicit conversions.
 */
class WithFillTest extends DslTestSpec {

  private def sql(query: OperationalQuery): String = toSql(query.internalQuery, None)

  "WITH FILL" should "render bare" in {
    val query = select(col2).from(TwoTestTable).orderByColumns(OrderingColumn(col2, ASC, Option(WithFill())))
    sql(query) should matchSQL(s"SELECT column_2 FROM ${TwoTestTable.quoted} ORDER BY column_2 ASC WITH FILL")
  }

  it should "render FROM, TO and STEP in that order" in {
    val fill  = WithFill(from = Option(const(1)), to = Option(const(6)), step = Option(const(2)))
    val query = select(col2).from(TwoTestTable).orderByColumns(OrderingColumn(col2, ASC, Option(fill)))
    sql(query) should matchSQL(
      s"SELECT column_2 FROM ${TwoTestTable.quoted} ORDER BY column_2 ASC WITH FILL FROM 1 TO 6 STEP 2"
    )
  }

  it should "render only the bounds that were given" in {
    val query = select(col2)
      .from(TwoTestTable)
      .orderByColumns(
        OrderingColumn(col2, DESC, Option(WithFill(step = Option(const(5)))))
      )
    sql(query) should matchSQL(
      s"SELECT column_2 FROM ${TwoTestTable.quoted} ORDER BY column_2 DESC WITH FILL STEP 5"
    )
  }

  it should "render STALENESS" in {
    val fill  = WithFill(to = Option(const(6)), staleness = Option(const(3)))
    val query = select(col2).from(TwoTestTable).orderByColumns(OrderingColumn(col2, ASC, Option(fill)))
    sql(query) should matchSQL(
      s"SELECT column_2 FROM ${TwoTestTable.quoted} ORDER BY column_2 ASC WITH FILL TO 6 STALENESS 3"
    )
  }

  // The server rejects this pairing with INVALID_WITH_FILL_EXPRESSION; refusing to build it says so sooner.
  it should "refuse STALENESS together with FROM" in {
    an[IllegalArgumentException] should be thrownBy
    WithFill(from = Option(const(1)), staleness = Option(const(3)))
  }

  it should "attach per column, leaving the others alone" in {
    val query = select(itemId, col2)
      .from(TwoTestTable)
      .orderByColumns(OrderingColumn(itemId), OrderingColumn(col2, ASC, Option(WithFill())))
    sql(query) should matchSQL(
      s"SELECT item_id, column_2 FROM ${TwoTestTable.quoted} ORDER BY item_id ASC, column_2 ASC WITH FILL"
    )
  }

  "INTERPOLATE" should "render bare when given no columns" in {
    val query = select(col2, col3)
      .from(TwoTestTable)
      .orderByColumns(OrderingColumn(col2, ASC, Option(WithFill())))
      .interpolate()
    sql(query) should matchSQL(
      s"SELECT column_2, column_3 FROM ${TwoTestTable.quoted} ORDER BY column_2 ASC WITH FILL INTERPOLATE"
    )
  }

  it should "name columns" in {
    val query = select(col2, col3)
      .from(TwoTestTable)
      .orderByColumns(OrderingColumn(col2, ASC, Option(WithFill())))
      .interpolate(InterpolateColumn(col3))
    sql(query) should matchSQL(
      s"SELECT column_2, column_3 FROM ${TwoTestTable.quoted} ORDER BY column_2 ASC WITH FILL INTERPOLATE (column_3)"
    )
  }

  it should "carry an expression for a column" in {
    val query = select(col2, col3)
      .from(TwoTestTable)
      .orderByColumns(OrderingColumn(col2, ASC, Option(WithFill())))
      .interpolate(InterpolateColumn(col3, Option(col3)))
    sql(query) should matchSQL(
      s"SELECT column_2, column_3 FROM ${TwoTestTable.quoted} " +
        "ORDER BY column_2 ASC WITH FILL INTERPOLATE (column_3 AS column_3)"
    )
  }

  it should "come after ORDER BY and before LIMIT" in {
    val query = select(col2, col3)
      .from(TwoTestTable)
      .orderByColumns(OrderingColumn(col2, ASC, Option(WithFill())))
      .interpolate(InterpolateColumn(col3))
      .limit(Option(Limit(5)))
    sql(query) should matchSQL(
      s"SELECT column_2, column_3 FROM ${TwoTestTable.quoted} " +
        "ORDER BY column_2 ASC WITH FILL INTERPOLATE (column_3) LIMIT 0, 5"
    )
  }

  "the reshaped ordering" should "still accept a bare column" in {
    sql(select(col2).from(TwoTestTable).orderBy(col2)) should matchSQL(
      s"SELECT column_2 FROM ${TwoTestTable.quoted} ORDER BY column_2 ASC"
    )
  }

  it should "still accept a (column, direction) tuple" in {
    sql(select(col2).from(TwoTestTable).orderByWithDirection((col2, DESC))) should matchSQL(
      s"SELECT column_2 FROM ${TwoTestTable.quoted} ORDER BY column_2 DESC"
    )
  }

  it should "convert a tuple implicitly where an OrderingColumn is expected" in {
    val converted: OrderingColumn = (col2, DESC)
    converted shouldBe OrderingColumn(col2, DESC, None)
  }

  it should "convert a bare column implicitly, defaulting to ASC" in {
    val converted: OrderingColumn = col2
    converted shouldBe OrderingColumn(col2, ASC, None)
  }
}
