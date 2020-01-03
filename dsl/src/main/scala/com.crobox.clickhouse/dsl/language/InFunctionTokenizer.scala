package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.dongxiguo.fastring.Fastring.Implicits._

trait InFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeInFunction(col: InFunction): String = col match {
    case t: Tuple => fast"(${t.coln.map(col => tokenizeColumn(col.column)).mkFastring(", ")})" // Tuple Creation Operator
    case t: TupleElement[_] => fast"${tokenizeColumn(t.tuple.column)}.${tokenizeColumn(t.index.column)})" // Access Operators
    case col: InFunctionCol[_] => tokenizeInFunctionCol(col)
  }

  private def tokenizeInFunctionCol(col: InFunctionCol[_]): String = col match {
    case In(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      fast"${tokenizeColumn(l.column)} IN ${tokenizeInFunRHCol(r)}"
    case NotIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      fast"${tokenizeColumn(l.column)} NOT IN ${tokenizeInFunRHCol(r)}"
    case GlobalIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      fast"${tokenizeColumn(l.column)} GLOBAL IN ${tokenizeInFunRHCol(r)}"
    case GlobalNotIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      fast"${tokenizeColumn(l.column)} GLOBAL NOT IN ${tokenizeInFunRHCol(r)}"
  }

  private def tokenizeInFunRHCol(value: InFuncRHMagnet) = value match {
    case col: InFuncRHMagnet if col.query.isDefined => fast"(${toRawSql(col.query.get.internalQuery)})"
    case col: InFuncRHMagnet if col.tableRef.isDefined => col.tableRef.get.quoted
    case col: InFuncRHMagnet => tokenizeColumn(col.column)
  }

}
