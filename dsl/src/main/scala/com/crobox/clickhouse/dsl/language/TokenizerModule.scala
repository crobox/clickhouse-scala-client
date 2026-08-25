package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl.{ExplainKind, InternalQuery}

trait TokenizerModule {

  def toSql(query: InternalQuery, formatting: Option[String] = Some("JSON"))(implicit ctx: TokenizeContext): String

  /**
   * `EXPLAIN <kind> [options] <query>`, with no `FORMAT` of its own.
   *
   * A separate entry point rather than a clause on [[InternalQuery]], because EXPLAIN wraps a whole statement: as a
   * field it would render inside subquery parentheses and survive a query merge.
   *
   * No formatting parameter, because a trailing `FORMAT` does not mean the same thing for every kind --
   * [[ExplainKind.Ast]] reports the entire parsed statement, so it folds the `FORMAT` into what it explains rather than
   * using it. Callers that need to parse the result should select from this as a subquery, which is what
   * `QueryExecutor.explainRows` does.
   */
  def toExplainSql(
      kind: ExplainKind,
      query: InternalQuery,
      options: Seq[(String, String)] = Seq.empty
  )(implicit ctx: TokenizeContext): String
}
