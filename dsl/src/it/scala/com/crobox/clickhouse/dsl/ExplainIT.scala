package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslITSpec

/**
 * Every `EXPLAIN` kind against a real server. A tokenizer arm is a string template, so the only real check that a
 * kind's keyword and option placement are right is the server accepting them.
 */
class ExplainIT extends DslITSpec {

  // Valid on purpose: the analysing kinds reject a query whose projection is neither grouped nor aggregated, which is
  // itself a decent advertisement for EXPLAIN.
  private val query = select(itemId).from(TwoTestTable).where(col3 isEq "x").groupBy(itemId)

  // Asserted on the declared columns rather than on rows: ESTIMATE reports parts and marks, which a Memory table has
  // none of, so it legitimately returns a header and no rows here.
  it should "be accepted by the server for every kind" in
    ExplainKind.All.foreach { kind =>
      withClue(s"EXPLAIN ${kind.keyword}: ") {
        queryExecutor.explainRows(kind, query).futureValue.meta.map(_.columnTypes) should not be empty
      }
    }

  it should "report the plan as text" in {
    val lines = queryExecutor.explain(ExplainKind.Plan, query).futureValue
    lines should not be empty
    // Asserting the whole tree would pin this to a server version; the outermost step is stable.
    lines.head should include("Expression")
  }

  it should "report the parsed statement for AST, which cannot carry a FORMAT of its own" in {
    val lines = queryExecutor.explain(ExplainKind.Ast, query).futureValue
    lines.head should include("SelectWithUnionQuery")
  }

  it should "rewrite the query back to SQL for SYNTAX" in {
    val sql = queryExecutor.explain(ExplainKind.Syntax, query).futureValue.mkString(" ")
    sql should include("SELECT")
    sql should include("item_id")
  }

  it should "expose ESTIMATE's columns, which are not a single explain column" in {
    val meta = queryExecutor.explainRows(ExplainKind.Estimate, query).futureValue.meta
    meta.map(_.columnTypes.map(_.name)).getOrElse(Seq.empty) should
    contain allOf ("database", "table", "parts", "rows", "marks")
  }

  it should "report nothing through the text accessor for ESTIMATE" in {
    queryExecutor.explain(ExplainKind.Estimate, query).futureValue shouldBe empty
  }

  it should "pass options through to the server" in {
    queryExecutor.explain(ExplainKind.Plan, query, Seq("indexes" -> "1")).futureValue should not be empty
  }

  it should "surface an option the server rejects rather than dropping it" in {
    queryExecutor.explain(ExplainKind.Plan, query, Seq("not_a_setting" -> "1")).failed.futureValue should not be null
  }

  it should "explain a query carrying the clauses added for #328" in {
    val clauses = select(shieldId)
      .from(OneTestTable)
      .withArrayJoin(numbers as "n")
      .where(shieldId isEq "a")
      .limit(Option(Limit(5)))
      .settings("max_threads" -> "1")
    queryExecutor.explain(ExplainKind.Syntax, clauses).futureValue should not be empty
  }
}
