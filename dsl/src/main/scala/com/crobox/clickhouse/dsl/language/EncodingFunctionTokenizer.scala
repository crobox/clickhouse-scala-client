package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait EncodingFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeEncodingFunction(col: EncodingFunction[_])(implicit ctx: TokenizeContext): String = col match {
    case Hex(col: HexCompatible[_])               => s"hex(${tokenizeColumn(col.column)})"
    case Unhex(col: StringColMagnet[_])           => s"unhex(${tokenizeColumn(col.column)})"
    case UUIDStringToNum(col: StringColMagnet[_]) => s"UUIDStringToNum(${tokenizeColumn(col.column)})"
    case UUIDNumToString(col: StringColMagnet[_]) => s"UUIDNumToString(${tokenizeColumn(col.column)})"
    case BitmaskToList(col: NumericCol[_])        => s"bitmaskToList(${tokenizeColumn(col.column)})"
    case BitmaskToArray(col: NumericCol[_])       => s"bitmaskToArray(${tokenizeColumn(col.column)})"
  }
}
