package com.crobox.clickhouse.dsl.language

import com.dongxiguo.fastring.Fastring.Implicits._
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database

trait LogicalFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeLogicalFunction(col: ExpressionColumn[Boolean])(implicit database: Database): String = col match {
    case Not(col: NumericCol[_]) =>
      fast"not(${tokenizeColumn(col.column)})"
    case col: LogicalFunction =>
      tokenizeLogicalFunction(col)
  }
  def tokenizeLogicalFunction(col: LogicalFunction)(implicit database: Database): String = col match {
    case And(_left: NumericCol[_], _right: NumericCol[_]) =>
      fast"${tokenizeColumn(_left.column)} AND ${tokenizeColumn(_right.column)}"
    case Or(_left: NumericCol[_], _right: NumericCol[_]) =>
      fast"(${tokenizeColumn(_left.column)} OR ${tokenizeColumn(_right.column)})"
    case Xor(_left: NumericCol[_], _right: NumericCol[_]) =>
      fast"xor(${tokenizeColumn(_left.column)}, ${tokenizeColumn(_right.column)})"
  }

}
