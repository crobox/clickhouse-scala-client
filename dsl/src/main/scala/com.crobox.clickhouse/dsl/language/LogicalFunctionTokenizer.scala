package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database
import com.dongxiguo.fastring.Fastring.Implicits._

trait LogicalFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeLogicalFunction(col: ExpressionColumn[Boolean])(implicit database: Database): String = col match {
    case Not(col: NumericCol[_]) =>
      fast"not(${tokenizeColumn(col.column)})"
    case col: LogicalFunction =>
      tokenizeLogicalFunction(col)
  }

  def tokenizeLogicalFunction(col: LogicalFunction)(implicit database: Database): String =
    (col.left.asOption, col.right.asOption) match {
      case (None, None) => "1"
      case (Some(left), None) => tokenizeColumn(left)
      case (None, Some(right)) => tokenizeColumn(right)
      case (Some(left), Some(right)) => col match {
        case _: And => fast"${tokenizeColumn(left)} AND ${tokenizeColumn(right)}"
        case _: Or => fast"(${tokenizeColumn(left)} OR ${tokenizeColumn(right)})"
        case _: Xor => fast"xor(${tokenizeColumn(left)}, ${tokenizeColumn(right)})"
      }
    }

}
