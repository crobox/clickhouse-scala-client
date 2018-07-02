package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.dongxiguo.fastring.Fastring.Implicits._


trait EncodingFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeEncodingFunction(col: EncodingFunction[_]): String = col match {
    case Hex(col: HexCompatible) => fast"hex(${tokenizeColumn(col.column)})"
    case Unhex(col: StringColMagnet) => fast"unhex(${tokenizeColumn(col.column)})"
    case UUIDStringToNum(col: StringColMagnet) => fast"UUIDStringToNum(${tokenizeColumn(col.column)})"
    case UUIDNumToString(col: StringColMagnet) => fast"UUIDNumToString(${tokenizeColumn(col.column)})"
    case BitmaskToList(col: NumericCol) => fast"bitmaskToList(${tokenizeColumn(col.column)})"
    case BitmaskToArray(col: NumericCol) => fast"bitmaskToArray(${tokenizeColumn(col.column)})"
  }
}
