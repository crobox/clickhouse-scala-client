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
      s"${tokenizeColumn(l.column)} IN ${tokenizeInFunRHCol(r, () => ctx.setTableAlias(r.query.forall(_.internalQuery.join.isEmpty)))}"
    case NotIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      s"${tokenizeColumn(l.column)} NOT IN ${tokenizeInFunRHCol(r, () => ctx.setTableAlias(r.query.forall(_.internalQuery.join.isEmpty)))}"
    case GlobalIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      s"${tokenizeColumn(l.column)} GLOBAL IN ${tokenizeInFunRHCol(r, () => ctx)}"
    case GlobalNotIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      s"${tokenizeColumn(l.column)} GLOBAL NOT IN ${tokenizeInFunRHCol(r, () => ctx)}"
  }

  private def tokenizeInFunRHCol(value: InFuncRHMagnet, fn: () => TokenizeContext)(implicit
      ctx: TokenizeContext
  ): String =
    value match {
      case col: InFuncRHMagnet if col.query.isDefined =>
        s"(${toRawSql(col.query.get.internalQuery)(fn())})"
      case col: InFuncRHMagnet if col.tableRef.isDefined =>
        col.tableRef.map(table => table.quoted + fn().tableAlias(table)).get
      case col: InFuncRHMagnet => tokenizeColumn(col.column)
    }
}
