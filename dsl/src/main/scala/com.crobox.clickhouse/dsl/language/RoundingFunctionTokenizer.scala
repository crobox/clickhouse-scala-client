package com.crobox.clickhouse.dsl.language

import com.dongxiguo.fastring.Fastring.Implicits._
import com.crobox.clickhouse.dsl._

trait RoundingFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeRoundingFunction(col: RoundingFunction): String = col match {
    case Floor(col: NumericCol, n: NumericCol) => fast"floor(${tokenizeColumn(col.column)},${tokenizeColumn(n.column)})"
    case Ceil(col: NumericCol, n: NumericCol) => fast"ceil(${tokenizeColumn(col.column)},${tokenizeColumn(n.column)})"
    case Round(col: NumericCol, n: NumericCol) => fast"round(${tokenizeColumn(col.column)},${tokenizeColumn(n.column)})"
    case RoundToExp2(col: NumericCol) => fast"roundToExp2(${tokenizeColumn(col.column)})"
    case RoundDuration(col: NumericCol) => fast"roundDuration(${tokenizeColumn(col.column)})"
    case RoundAge(col: NumericCol) => fast"roundAge(${tokenizeColumn(col.column)})"

  }

}
