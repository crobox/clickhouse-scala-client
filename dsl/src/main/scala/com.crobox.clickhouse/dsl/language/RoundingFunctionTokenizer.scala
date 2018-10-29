package com.crobox.clickhouse.dsl.language

import com.dongxiguo.fastring.Fastring.Implicits._
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database

trait RoundingFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeRoundingFunction(col: RoundingFunction)(implicit database: Database): String = col match {
    case Floor(col: NumericCol[_], n: NumericCol[_]) => fast"floor(${tokenizeColumn(col.column)},${tokenizeColumn(n.column)})"
    case Ceil(col: NumericCol[_], n: NumericCol[_]) => fast"ceil(${tokenizeColumn(col.column)},${tokenizeColumn(n.column)})"
    case Round(col: NumericCol[_], n: NumericCol[_]) => fast"round(${tokenizeColumn(col.column)},${tokenizeColumn(n.column)})"
    case RoundToExp2(col: NumericCol[_]) => fast"roundToExp2(${tokenizeColumn(col.column)})"
    case RoundDuration(col: NumericCol[_]) => fast"roundDuration(${tokenizeColumn(col.column)})"
    case RoundAge(col: NumericCol[_]) => fast"roundAge(${tokenizeColumn(col.column)})"

  }

}
