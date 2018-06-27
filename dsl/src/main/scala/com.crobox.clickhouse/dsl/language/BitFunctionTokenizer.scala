package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.dongxiguo.fastring.Fastring.Implicits._

trait BitFunctionTokenizer { this: ClickhouseTokenizerModule => 
  def tokenizeBitFunction(col: BitFunction): String = col match {
    case BitAnd(a: NumericCol, b: NumericCol) => fast"bitAnd(${tokenizeColumn(a.column)}${tokenizeColumn(b.column)})" 
    case BitOr(a: NumericCol, b: NumericCol) => fast"bitOr(${tokenizeColumn(a.column)}${tokenizeColumn(b.column)})" 
    case BitXor(a: NumericCol, b: NumericCol) => fast"bitXor(${tokenizeColumn(a.column)}${tokenizeColumn(b.column)})" 
    case BitNot(a: NumericCol) => fast"bitNot(${tokenizeColumn(a.column)})" 
    case BitShiftLeft(a: NumericCol, b: NumericCol) => fast"bitShiftLeft(${tokenizeColumn(a.column)}${tokenizeColumn(b.column)})" 
    case BitShiftRight(a: NumericCol, b: NumericCol) => fast"bitShiftRight(${tokenizeColumn(a.column)}${tokenizeColumn(b.column)})" 
  }
}
