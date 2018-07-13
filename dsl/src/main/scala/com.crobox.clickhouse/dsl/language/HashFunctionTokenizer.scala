package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl._
import com.dongxiguo.fastring.Fastring.Implicits._

trait HashFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeHashFunction(col: HashFunction): String = col match {
    case HalfMD5(col: StringColMagnet)    => fast"halfMD5(${tokenizeColumn(col.column)})"
    case MD5(col: StringColMagnet)        => fast"mD5(${tokenizeColumn(col.column)})"
    case SipHash64(col: StringColMagnet)  => fast"sipHash64(${tokenizeColumn(col.column)})"
    case SipHash128(col: StringColMagnet) => fast"sipHash128(${tokenizeColumn(col.column)})"
    case CityHash64(col1: ConstOrColMagnet, coln: Seq[ConstOrColMagnet]) =>
      fast"cityHash64(${tokenizeColumn(col1.column)},${tokenizeSeqCol(coln.map(_.column))}})"
    case IntHash32(col: NumericCol)    => fast"intHash32(${tokenizeColumn(col.column)})"
    case IntHash64(col: NumericCol)    => fast"intHash64(${tokenizeColumn(col.column)})"
    case SHA1(col: ConstOrColMagnet)   => fast"SHA1(${tokenizeColumn(col.column)})"
    case SHA224(col: ConstOrColMagnet) => fast"SHA224(${tokenizeColumn(col.column)})"
    case SHA256(col: ConstOrColMagnet) => fast"SHA256(${tokenizeColumn(col.column)})"
    case URLHash(col: ConstOrColMagnet, depth: NumericCol) =>
      fast"URLHash(${tokenizeColumn(col.column)},${tokenizeColumn(depth.column)})"
  }

}
