package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait RoundingFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeRoundingFunction(col: RoundingFunction): String = col match {
    case Floor(col: NumericCol[_], n: NumericCol[_]) => s"floor(${tokenizeColumn(col.column)},${tokenizeColumn(n.column)})"
    case Ceil(col: NumericCol[_], n: NumericCol[_]) => s"ceil(${tokenizeColumn(col.column)},${tokenizeColumn(n.column)})"
    case Round(col: NumericCol[_], n: NumericCol[_]) => s"round(${tokenizeColumn(col.column)},${tokenizeColumn(n.column)})"
    case RoundToExp2(col: NumericCol[_]) => s"roundToExp2(${tokenizeColumn(col.column)})"
    case RoundDuration(col: NumericCol[_]) => s"roundDuration(${tokenizeColumn(col.column)})"
    case RoundAge(col: NumericCol[_]) => s"roundAge(${tokenizeColumn(col.column)})"

  }

}
