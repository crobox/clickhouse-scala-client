package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait BitFunctionTokenizer { this: ClickhouseTokenizerModule => 
  def tokenizeBitFunction(col: BitFunction): String = col match {
    case BitAnd(a: NumericCol[_], b: NumericCol[_]) => s"bitAnd(${tokenizeColumn(a.column)},${tokenizeColumn(b.column)})"
    case BitOr(a: NumericCol[_], b: NumericCol[_]) => s"bitOr(${tokenizeColumn(a.column)},${tokenizeColumn(b.column)})"
    case BitXor(a: NumericCol[_], b: NumericCol[_]) => s"bitXor(${tokenizeColumn(a.column)},${tokenizeColumn(b.column)})"
    case BitNot(a: NumericCol[_]) => s"bitNot(${tokenizeColumn(a.column)})"
    case BitShiftLeft(a: NumericCol[_], b: NumericCol[_]) => s"bitShiftLeft(${tokenizeColumn(a.column)},${tokenizeColumn(b.column)})"
    case BitShiftRight(a: NumericCol[_], b: NumericCol[_]) => s"bitShiftRight(${tokenizeColumn(a.column)},${tokenizeColumn(b.column)})"
  }
}
