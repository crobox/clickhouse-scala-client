package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database
import com.dongxiguo.fastring.Fastring.Implicits._

trait InFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeInFunction(col: InFunction)(implicit db: Database): String = col match {
    case t: Tuple => fast"tuple(${t.coln.map(col => tokenizeColumn(col.column)).mkString(",")})"
    case t: TupleElement[_] => fast"tuple(${tokenizeColumn(t.tuple.column)},${tokenizeColumn(t.index.column)})"
    case col: InFunctionCol[_] => tokenizeInFunctionCol(col)
  }

  private def tokenizeInFunctionCol(col: InFunctionCol[_])(implicit db: Database) = col match {
    case In(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      fast"${tokenizeColumn(l.column)} IN ${tokenizeInFunRHCol(r)}"
    case NotIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      fast"${tokenizeColumn(l.column)} NOT IN ${tokenizeInFunRHCol(r)}"
    case GlobalIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      fast"${tokenizeColumn(l.column)} GLOBAL IN ${tokenizeInFunRHCol(r)}"
    case GlobalNotIn(l: ConstOrColMagnet[_], r: InFuncRHMagnet) =>
      fast"${tokenizeColumn(l.column)} GLOBAL NOT IN ${tokenizeInFunRHCol(r)}"
  }

  private def tokenizeInFunRHCol(value: InFuncRHMagnet)(implicit db: Database) = value match {
    case col: InFuncRHMagnet if col.query.isDefined => toRawSql(col.query.get.internalQuery)
    case col: InFuncRHMagnet if col.tableRef.isDefined => col.tableRef.get.name
    case col: InFuncRHMagnet => tokenizeColumn(col.column)
  }

}
