package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslTestSpec

class WindowFunctionTest extends DslTestSpec {

  private def sql(query: OperationalQuery): String = toSql(query.internalQuery, None)

  "OVER" should "render an empty window" in {
    sql(select(sum(col2).over()).from(TwoTestTable)) should matchSQL(
      s"SELECT sum(column_2) OVER () FROM ${TwoTestTable.quoted}"
    )
  }

  it should "render a partition" in {
    sql(select(sum(col2).over(WindowSpec(partitionBy = Seq(col3)))).from(TwoTestTable)) should matchSQL(
      s"SELECT sum(column_2) OVER (PARTITION BY column_3) FROM ${TwoTestTable.quoted}"
    )
  }

  it should "render a partition and an ordering" in {
    val spec = WindowSpec(partitionBy = Seq(col3), orderBy = Seq(OrderingColumn(col2)))
    sql(select(sum(col2).over(spec)).from(TwoTestTable)) should matchSQL(
      s"SELECT sum(column_2) OVER (PARTITION BY column_3 ORDER BY column_2 ASC) FROM ${TwoTestTable.quoted}"
    )
  }

  it should "render a two-sided ROWS frame" in {
    val spec = WindowSpec(
      orderBy = Seq(OrderingColumn(col2)),
      frame = Option(WindowFrame(FrameMode.Rows, FrameBound.Preceding(1), Option(FrameBound.CurrentRow)))
    )
    sql(select(sum(col2).over(spec)).from(TwoTestTable)) should matchSQL(
      s"SELECT sum(column_2) OVER (ORDER BY column_2 ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) " +
        s"FROM ${TwoTestTable.quoted}"
    )
  }

  it should "render an unbounded RANGE frame" in {
    val spec = WindowSpec(
      orderBy = Seq(OrderingColumn(col2)),
      frame = Option(
        WindowFrame(FrameMode.Range, FrameBound.UnboundedPreceding, Option(FrameBound.UnboundedFollowing))
      )
    )
    sql(select(sum(col2).over(spec)).from(TwoTestTable)) should matchSQL(
      s"SELECT sum(column_2) OVER (ORDER BY column_2 ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) " +
        s"FROM ${TwoTestTable.quoted}"
    )
  }

  it should "render a single-bound frame without BETWEEN" in {
    val spec = WindowSpec(
      orderBy = Seq(OrderingColumn(col2)),
      frame = Option(WindowFrame(FrameMode.Rows, FrameBound.UnboundedPreceding))
    )
    sql(select(sum(col2).over(spec)).from(TwoTestTable)) should matchSQL(
      s"SELECT sum(column_2) OVER (ORDER BY column_2 ASC ROWS UNBOUNDED PRECEDING) FROM ${TwoTestTable.quoted}"
    )
  }

  "WINDOW" should "define a window a column can refer to by name" in {
    val w = NamedWindow("w", WindowSpec(partitionBy = Seq(col3)))
    sql(select(sum(col2).over(w)).from(TwoTestTable).window(w)) should matchSQL(
      s"SELECT sum(column_2) OVER w FROM ${TwoTestTable.quoted} WINDOW w AS (PARTITION BY column_3)"
    )
  }

  it should "define several windows" in {
    val a = NamedWindow("a", WindowSpec(partitionBy = Seq(col3)))
    val b = NamedWindow("b", WindowSpec(orderBy = Seq(OrderingColumn(col2))))
    sql(select(sum(col2).over(a)).from(TwoTestTable).window(a, b)) should matchSQL(
      s"SELECT sum(column_2) OVER a FROM ${TwoTestTable.quoted} " +
        "WINDOW a AS (PARTITION BY column_3), b AS (ORDER BY column_2 ASC)"
    )
  }

  "QUALIFY" should "filter on a window result" in {
    val query = select(sum(col2).over() as "s").from(TwoTestTable).qualify(ref[Long]("s") > 1)
    sql(query) should matchSQL(
      s"SELECT sum(column_2) OVER () AS s FROM ${TwoTestTable.quoted} QUALIFY s > 1"
    )
  }

  it should "combine repeated conditions with AND" in {
    val query = select(sum(col2).over() as "s")
      .from(TwoTestTable)
      .qualify(ref[Long]("s") > 1)
      .qualify(ref[Long]("s") < 10)
    sql(query) should matchSQL(
      s"SELECT sum(column_2) OVER () AS s FROM ${TwoTestTable.quoted} QUALIFY s > 1 AND s < 10"
    )
  }

  // Documented order: HAVING, then WINDOW, then QUALIFY, then ORDER BY.
  it should "sit between HAVING and ORDER BY" in {
    val w     = NamedWindow("w", WindowSpec(partitionBy = Seq(col3)))
    val query = select(col3, sum(col2).over(w) as "s")
      .from(TwoTestTable)
      .groupBy(col3)
      .having(col3 isEq "x")
      .window(w)
      .qualify(ref[Long]("s") > 1)
      .orderBy(col3)
    sql(query) should matchSQL(
      s"SELECT column_3, sum(column_2) OVER w AS s FROM ${TwoTestTable.quoted} GROUP BY column_3 " +
        "HAVING column_3 = 'x' WINDOW w AS (PARTITION BY column_3) QUALIFY s > 1 ORDER BY column_3 ASC"
    )
  }

  // ClickHouse rejects `v OVER ()`, `1 OVER ()` and `toUInt32(v) OVER ()` -- it wants an aggregate before OVER -- so
  // `over` lives on AggregateFunction and these are compile errors rather than server errors.
  "OVER" should "not be available on a bare column" in {
    """col2.over()""" shouldNot typeCheck
  }

  it should "not be available on a literal" in {
    """const(1).over()""" shouldNot typeCheck
  }

  it should "not be available on a non-aggregate function" in {
    """toUInt32(col2).over()""" shouldNot typeCheck
  }

  it should "be available on any aggregate" in {
    sql(select(count().over()).from(TwoTestTable)) should matchSQL(
      s"SELECT count() OVER () FROM ${TwoTestTable.quoted}"
    )
  }

  "A window-only function" should "render row_number" in {
    sql(select(rowNumber().over(WindowSpec(orderBy = Seq(col2)))).from(TwoTestTable)) should matchSQL(
      s"SELECT row_number() OVER (ORDER BY column_2 ASC) FROM ${TwoTestTable.quoted}"
    )
  }

  it should "render the other rankings" in {
    sql(select(rank().over(), denseRank().over(), percentRank().over(), ntile(4).over()).from(TwoTestTable)) should
    matchSQL(
      s"SELECT rank() OVER (), dense_rank() OVER (), percent_rank() OVER (), ntile(4) OVER () " +
        s"FROM ${TwoTestTable.quoted}"
    )
  }

  it should "render lagInFrame at each arity" in {
    sql(
      select(
        lagInFrame(col2).over(),
        lagInFrame(col2, Option(const(2))).over(),
        lagInFrame(col2, Option(const(2)), Option(const(99))).over()
      ).from(TwoTestTable)
    ) should matchSQL(
      s"SELECT lagInFrame(column_2) OVER (), lagInFrame(column_2, 2) OVER (), " +
        s"lagInFrame(column_2, 2, 99) OVER () FROM ${TwoTestTable.quoted}"
    )
  }

  it should "render leadInFrame and nth_value" in {
    sql(select(leadInFrame(col2, Option(const(1))).over(), nthValue(col2, 2).over()).from(TwoTestTable)) should
    matchSQL(
      s"SELECT leadInFrame(column_2, 1) OVER (), nth_value(column_2, 2) OVER () FROM ${TwoTestTable.quoted}"
    )
  }

  it should "work through a named window" in {
    val w = NamedWindow("w", WindowSpec(partitionBy = Seq(col3), orderBy = Seq(col2)))
    sql(select(rowNumber().over(w)).from(TwoTestTable).window(w)) should matchSQL(
      s"SELECT row_number() OVER w FROM ${TwoTestTable.quoted} " +
        s"WINDOW w AS (PARTITION BY column_3 ORDER BY column_2 ASC)"
    )
  }

  // The server rejects `row_number()` on its own with "can only be used as a window function", so the builders return
  // something that is not a Column and `over` is the only way out of it.
  it should "not be selectable without a window" in {
    """select(rowNumber())""" shouldNot typeCheck
  }

  it should "refuse a non-positive ntile bucket count" in {
    an[IllegalArgumentException] should be thrownBy ntile(0)
  }

  it should "refuse a zero nth_value offset" in {
    an[IllegalArgumentException] should be thrownBy nthValue(col2, 0)
  }

  it should "refuse a default without an offset" in {
    an[IllegalArgumentException] should be thrownBy lagInFrame(col2, None, Option(const(1)))
  }

  // The server parses WITH FILL inside OVER and then ignores it: no error, no filled rows. Refused at construction
  // rather than emitting a clause that does nothing.
  "A window spec" should "refuse an ordering that carries WITH FILL" in {
    an[IllegalArgumentException] should be thrownBy
    WindowSpec(orderBy = Seq(OrderingColumn(col2, ASC, Option(WithFill()))))
  }

  it should "accept an ordering without one" in {
    WindowSpec(orderBy = Seq(OrderingColumn(col2))).orderBy should have size 1
  }
}
