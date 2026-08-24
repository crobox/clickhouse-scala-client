package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslTestSpec

/**
 * `EXPLAIN` rendering. It wraps a statement rather than sitting inside one, so what matters here is that the wrapped
 * query is unchanged and that the prefix cannot leak into a subquery.
 */
class ExplainTest extends DslTestSpec {

  private val query = select(shieldId).from(OneTestTable).where(shieldId isEq "a")

  "EXPLAIN" should "prefix the query with the kind's keyword" in {
    toExplainSql(ExplainKind.Plan, query.internalQuery) should matchSQL(
      s"EXPLAIN PLAN SELECT shield_id FROM ${OneTestTable.quoted} WHERE shield_id = 'a'"
    )
  }

  // That each of these is actually accepted is checked against a live server in ExplainIT.
  it should "spell every kind the way ClickHouse does" in {
    ExplainKind.All.map(_.keyword) should contain theSameElementsInOrderAs
    Seq("AST", "SYNTAX", "QUERY TREE", "PLAN", "PIPELINE", "ESTIMATE")
  }

  it should "put the kind's keyword straight after EXPLAIN" in
    ExplainKind.All.foreach { kind =>
      toExplainSql(kind, select(const(1)).internalQuery) should
      startWith(s"EXPLAIN ${kind.keyword} ")
    }

  it should "render options before the query, not after it" in {
    val sql = toExplainSql(ExplainKind.Plan, query.internalQuery, Seq("indexes" -> "1"))
    sql should matchSQL(
      s"EXPLAIN PLAN indexes = 1 SELECT shield_id FROM ${OneTestTable.quoted} WHERE shield_id = 'a'"
    )
  }

  it should "separate several options" in {
    val sql = toExplainSql(ExplainKind.Plan, query.internalQuery, Seq("header" -> "1", "indexes" -> "1"))
    sql should matchSQL(
      s"EXPLAIN PLAN header = 1, indexes = 1 SELECT shield_id FROM ${OneTestTable.quoted} WHERE shield_id = 'a'"
    )
  }

  // No FORMAT of its own: EXPLAIN AST reports the whole parsed statement, so a trailing FORMAT would become part of
  // what it explains. QueryExecutor.explainRows selects from this as a subquery instead.
  it should "not append a FORMAT" in {
    toExplainSql(ExplainKind.Ast, select(const(1)).internalQuery) should not include "FORMAT"
  }

  // The reason EXPLAIN is not a field on InternalQuery: as one it would render inside these parentheses.
  it should "not leak into a subquery" in {
    val outer = select(shieldId).from(select(shieldId).from(OneTestTable))
    val sql   = toExplainSql(ExplainKind.Plan, outer.internalQuery)
    sql should matchSQL(
      s"EXPLAIN PLAN SELECT shield_id FROM (SELECT shield_id FROM ${OneTestTable.quoted})"
    )
    sql.sliding("EXPLAIN".length).count(_ == "EXPLAIN") shouldBe 1
  }

  it should "leave a clause-heavy query untouched" in {
    val full = select(itemId, col2)
      .from(TwoTestTable)
      .where(col3 isEq "x")
      .groupBy(itemId)
      .having(col2 isEq 1)
      .orderBy(itemId)
      .limit(Option(Limit(5)))
      .settings("max_threads" -> "1")
    toExplainSql(ExplainKind.Syntax, full.internalQuery) should matchSQL(
      s"EXPLAIN SYNTAX SELECT item_id, column_2 FROM ${TwoTestTable.quoted} WHERE column_3 = 'x' " +
        "GROUP BY item_id HAVING column_2 = 1 ORDER BY item_id ASC LIMIT 0, 5 SETTINGS max_threads = 1"
    )
  }
}
