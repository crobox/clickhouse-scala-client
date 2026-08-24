package com.crobox.clickhouse.dsl

/**
 * What an `EXPLAIN` should report.
 *
 * `EXPLAIN` is a statement that wraps a query rather than a clause inside one, which is why it is not a field on
 * [[InternalQuery]]: `toRawSql` recurses for subqueries, so a field would let a subquery render `EXPLAIN` inside its
 * own parentheses, and merging two queries would propagate it.
 *
 * https://clickhouse.com/docs/sql-reference/statements/explain
 */
sealed abstract class ExplainKind(val keyword: String)

object ExplainKind {

  /** The parsed statement, before any name resolution. Parses anything that is syntactically a query. */
  case object Ast extends ExplainKind("AST")

  /** The query rewritten back to SQL after analysis. */
  case object Syntax extends ExplainKind("SYNTAX")

  /** The analyzer's resolved tree: names, arity and argument types. Needs the analyzer, so 24.3 onwards. */
  case object QueryTree extends ExplainKind("QUERY TREE")

  /** The query plan. What a bare `EXPLAIN` returns. */
  case object Plan extends ExplainKind("PLAN")

  /** The execution pipeline. */
  case object Pipeline extends ExplainKind("PIPELINE")

  /** Estimated parts, rows and marks per table. The one kind whose result is not a single `explain` column. */
  case object Estimate extends ExplainKind("ESTIMATE")

  val All: Seq[ExplainKind] = Seq(Ast, Syntax, QueryTree, Plan, Pipeline, Estimate)
}
