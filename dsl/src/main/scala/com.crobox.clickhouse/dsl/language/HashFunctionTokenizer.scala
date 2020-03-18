package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait HashFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeHashFunction(col: HashFunction): String = col match {
    case HalfMD5(col: StringColMagnet[_])    => s"halfMD5(${tokenizeColumn(col.column)})"
    case MD5(col: StringColMagnet[_])        => s"MD5(${tokenizeColumn(col.column)})"
    case SipHash64(col: StringColMagnet[_])  => s"sipHash64(${tokenizeColumn(col.column)})"
    case SipHash128(col: StringColMagnet[_]) => s"sipHash128(${tokenizeColumn(col.column)})"
    case CityHash64(col1: ConstOrColMagnet[_], coln@_*) =>
      s"cityHash64(${tokenizeColumn(col1.column)}${tokenizeSeqCol(coln.map(_.column))})"
    case IntHash32(col: NumericCol[_])    => s"intHash32(${tokenizeColumn(col.column)})"
    case IntHash64(col: NumericCol[_])    => s"intHash64(${tokenizeColumn(col.column)})"
    case SHA1(col: ConstOrColMagnet[_])   => s"SHA1(${tokenizeColumn(col.column)})"
    case SHA224(col: ConstOrColMagnet[_]) => s"SHA224(${tokenizeColumn(col.column)})"
    case SHA256(col: ConstOrColMagnet[_]) => s"SHA256(${tokenizeColumn(col.column)})"
    case URLHash(col: ConstOrColMagnet[_], depth: NumericCol[_]) =>
      s"URLHash(${tokenizeColumn(col.column)},${tokenizeColumn(depth.column)})"
  }

}
