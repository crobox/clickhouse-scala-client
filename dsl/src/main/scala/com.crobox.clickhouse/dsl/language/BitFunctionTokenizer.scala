package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.dongxiguo.fastring.Fastring.Implicits._

trait BitFunctionTokenizer { this: ClickhouseTokenizerModule => 
  def tokenizeBitFunction(col: BitFunction): String = col match {
    case BitAnd(a: NumericCol[_], b: NumericCol[_]) => fast"bitAnd(${tokenizeColumn(a.column)}${tokenizeColumn(b.column)})" 
    case BitOr(a: NumericCol[_], b: NumericCol[_]) => fast"bitOr(${tokenizeColumn(a.column)}${tokenizeColumn(b.column)})" 
    case BitXor(a: NumericCol[_], b: NumericCol[_]) => fast"bitXor(${tokenizeColumn(a.column)}${tokenizeColumn(b.column)})" 
    case BitNot(a: NumericCol[_]) => fast"bitNot(${tokenizeColumn(a.column)})" 
    case BitShiftLeft(a: NumericCol[_], b: NumericCol[_]) => fast"bitShiftLeft(${tokenizeColumn(a.column)}${tokenizeColumn(b.column)})" 
    case BitShiftRight(a: NumericCol[_], b: NumericCol[_]) => fast"bitShiftRight(${tokenizeColumn(a.column)}${tokenizeColumn(b.column)})" 
  }
}
