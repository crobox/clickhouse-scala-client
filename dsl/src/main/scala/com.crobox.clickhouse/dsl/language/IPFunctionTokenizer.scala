package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait IPFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeIPFunction(col: IPFunction[_])(implicit ctx: TokenizeContext): String = col match {
    case IPv4NumToString(col: NumericCol[_])       => s"IPv4NumToString(${tokenizeColumn(col.column)})"
    case IPv4StringToNum(col: StringColMagnet[_])  => s"IPv4StringToNum(${tokenizeColumn(col.column)})"
    case IPv4NumToStringClassC(col: NumericCol[_]) => s"IPv4NumToStringClassC(${tokenizeColumn(col.column)})"
    case IPv6NumToString(col: StringColMagnet[_])  => s"IPv6NumToString(${tokenizeColumn(col.column)})"
    case IPv6StringToNum(col: StringColMagnet[_])  => s"IPv6StringToNum(${tokenizeColumn(col.column)})"
  }

}
