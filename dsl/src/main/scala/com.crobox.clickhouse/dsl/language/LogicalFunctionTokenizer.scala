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
    case _: And | _: Or =>
      if (col.left.asOption.isEmpty && col.right.asOption.isEmpty) "1"
      else if (col.left.asOption.isDefined && col.right.asOption.isEmpty) tokenizeColumn(col.left.asOption.get)
      else if (col.left.asOption.isEmpty && col.right.asOption.isDefined) tokenizeColumn(col.right.asOption.get)
      else {
        col match {
          case a: And => fast"${tokenizeColumn(col.left.asOption.get)} AND ${tokenizeColumn(col.right.asOption.get)}"
          case o: Or => fast"(${tokenizeColumn(col.left.asOption.get)} OR ${tokenizeColumn(col.right.asOption.get)})"
        }
      }
    case Or(left: LogicalOpsMagnet, right: LogicalOpsMagnet) =>
      if (left.asOption.isEmpty && right.asOption.isEmpty) "1"
      else if (left.asOption.isDefined && right.asOption.isEmpty) tokenizeColumn(left.asOption.get)
      else if (left.asOption.isEmpty && right.asOption.isDefined) tokenizeColumn(right.asOption.get)
      else fast"(${tokenizeColumn(left.asOption.get)} OR ${tokenizeColumn(right.asOption.get)})"
    case Xor(_left: LogicalOpsMagnet, _right: LogicalOpsMagnet) =>
      if (_right.asOption.isEmpty && _left.asOption.isEmpty) "1"
      else if (_left.asOption.isDefined && _right.asOption.isEmpty) tokenizeColumn(_left.asOption.get)
      else if (_left.asOption.isEmpty && _right.asOption.isDefined) tokenizeColumn(_right.asOption.get)
      else fast"xor(${tokenizeColumn(_left.asOption.get)},${tokenizeColumn(_right.asOption.get)})"
  }
}
