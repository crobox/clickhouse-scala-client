package com.crobox.clickhouse.dsl.language

import com.dongxiguo.fastring.Fastring.Implicits._

trait LogicalFunctionTokenizer {
  self: ClickhouseTokenizerModule =>
//TODO find the right place to add braces, so retain the right order of execution

  def tokenizeLogicalFunction(col: LogicalFunction): String = {
    case And(_left: NumericCol, _right: NumericCol) =>
      fast"and(${tokenizeColumn(_left.column)},${tokenizeColumn(_right.column)})"
    case Or(_left: NumericCol, _right: NumericCol) =>
      fast"or(${tokenizeColumn(_left.column)},${tokenizeColumn(_right.column)})"
    case Xor(_left: NumericCol, _right: NumericCol) =>
      fast"xor(${tokenizeColumn(_left.column)},${tokenizeColumn(_right.column)})"
  }

}
