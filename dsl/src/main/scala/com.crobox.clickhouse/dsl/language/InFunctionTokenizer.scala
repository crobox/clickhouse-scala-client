package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait InFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeInFunction(col: InFunction)(implicit ctx: TokenizeContext): String = col match {
    case t: Tuple => s"(${t.coln.map(col => tokenizeColumn(col.column)).mkString(", ")})" // Tuple Creation Operator
    case t: TupleElement[_] =>
      s"${tokenizeColumn(t.tuple.column)}.${tokenizeColumn(t.index.column)})" // Access Operators
    case col: InFunctionCol[_] => tokenizeInFunctionCol(col)
  }

  private def tokenizeInFunctionCol(col: InFunctionCol[_])(implicit ctx: TokenizeContext): String = col match {
    case In(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      s"${tokenizeColumn(l.column)} IN ${tokenizeInFunRHCol(r, true)}"
    case NotIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      s"${tokenizeColumn(l.column)} NOT IN ${tokenizeInFunRHCol(r, true)}"
    case GlobalIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      s"${tokenizeColumn(l.column)} GLOBAL IN ${tokenizeInFunRHCol(r, false)}"
    case GlobalNotIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      s"${tokenizeColumn(l.column)} GLOBAL NOT IN ${tokenizeInFunRHCol(r, false)}"
  }

  private def tokenizeInFunRHCol(value: InFuncRHMagnet, useTableAlias: Boolean)(implicit ctx: TokenizeContext): String =
    value match {
      case col: InFuncRHMagnet if col.query.isDefined =>
        // TODO: should we create a new context and simple 'reuse' the current?
        s"(${toRawSql(col.query.get.internalQuery)(ctx.copy(useTableAlias = useTableAlias))})"
      case col: InFuncRHMagnet if col.tableRef.isDefined =>
        col.tableRef.map(table => table.quoted + ctx.tableAlias(table)).get
      case col: InFuncRHMagnet => tokenizeColumn(col.column)
    }
}
