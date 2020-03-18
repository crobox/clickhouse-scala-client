package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait InFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeInFunction(col: InFunction): String = col match {
    case t: Tuple => s"(${t.coln.map(col => tokenizeColumn(col.column)).mkString(", ")})" // Tuple Creation Operator
    case t: TupleElement[_] => s"${tokenizeColumn(t.tuple.column)}.${tokenizeColumn(t.index.column)})" // Access Operators
    case col: InFunctionCol[_] => tokenizeInFunctionCol(col)
  }

  private def tokenizeInFunctionCol(col: InFunctionCol[_]): String = col match {
    case In(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      s"${tokenizeColumn(l.column)} IN ${tokenizeInFunRHCol(r)}"
    case NotIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      s"${tokenizeColumn(l.column)} NOT IN ${tokenizeInFunRHCol(r)}"
    case GlobalIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      s"${tokenizeColumn(l.column)} GLOBAL IN ${tokenizeInFunRHCol(r)}"
    case GlobalNotIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      s"${tokenizeColumn(l.column)} GLOBAL NOT IN ${tokenizeInFunRHCol(r)}"
  }

  private def tokenizeInFunRHCol(value: InFuncRHMagnet) = value match {
    case col: InFuncRHMagnet if col.query.isDefined => s"(${toRawSql(col.query.get.internalQuery)})"
    case col: InFuncRHMagnet if col.tableRef.isDefined => col.tableRef.get.quoted
    case col: InFuncRHMagnet => tokenizeColumn(col.column)
  }

}
