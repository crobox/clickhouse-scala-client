package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait InFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeInFunction(col: InFunction)(implicit ctx: TokenizeContext): String = col match {
    case t: Tuple => s"(${t.coln.map(col => tokenizeColumn(col.column)).mkString(", ")})" // Tuple Creation Operator
    // Access operator. The operand is parenthesised because it is not always a bare identifier -- for a map aggregate
    // it is a whole function call, and `sumMap(a, b).2` is a syntax error where `(sumMap(a, b)).2` is not.
    case t: TupleElement[_] =>
      s"(${tokenizeColumn(t.tuple.column)}).${tokenizeColumn(t.index.column)}"
    case col: InFunctionCol[_] => tokenizeInFunctionCol(col)
  }

  private def tokenizeInFunctionCol(col: InFunctionCol[_])(implicit ctx: TokenizeContext): String = col match {
    case In(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      s"${tokenizeColumn(l.column)} IN ${tokenizeInFunRHCol(r, aliasTables(r))}"
    case NotIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      s"${tokenizeColumn(l.column)} NOT IN ${tokenizeInFunRHCol(r, aliasTables(r))}"
    case GlobalIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      s"${tokenizeColumn(l.column)} GLOBAL IN ${tokenizeInFunRHCol(r, aliasTables = false)}"
    case GlobalNotIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      s"${tokenizeColumn(l.column)} GLOBAL NOT IN ${tokenizeInFunRHCol(r, aliasTables = false)}"
  }

  /** A right-hand side that joins already names its sides, so aliasing its tables again would clash. */
  private def aliasTables(r: InFuncRHMagnet): Boolean = r.query.forall(_.internalQuery.joins.isEmpty)

  private def tokenizeInFunRHCol(value: InFuncRHMagnet, aliasTables: Boolean)(implicit
      ctx: TokenizeContext
  ): String =
    value match {
      case col: InFuncRHMagnet if col.query.isDefined =>
        s"(${ctx.withTableAlias(aliasTables)(toRawSql(col.query.get.internalQuery))})"
      case col: InFuncRHMagnet if col.tableRef.isDefined =>
        col.tableRef.map(table => table.quoted + ctx.withTableAlias(aliasTables)(ctx.tableAlias(table))).get
      case col: InFuncRHMagnet => tokenizeColumn(col.column)
    }
}
