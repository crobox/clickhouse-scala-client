package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database
import com.dongxiguo.fastring.Fastring.Implicits._

trait EncodingFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeEncodingFunction(col: EncodingFunction[_])(implicit database: Database): String = col match {
    case Hex(col: HexCompatible[_])               => fast"hex(${tokenizeColumn(col.column)})"
    case Unhex(col: StringColMagnet[_])           => fast"unhex(${tokenizeColumn(col.column)})"
    case UUIDStringToNum(col: StringColMagnet[_]) => fast"UUIDStringToNum(${tokenizeColumn(col.column)})"
    case UUIDNumToString(col: StringColMagnet[_]) => fast"UUIDNumToString(${tokenizeColumn(col.column)})"
    case BitmaskToList(col: NumericCol[_])        => fast"bitmaskToList(${tokenizeColumn(col.column)})"
    case BitmaskToArray(col: NumericCol[_])       => fast"bitmaskToArray(${tokenizeColumn(col.column)})"
  }
}
