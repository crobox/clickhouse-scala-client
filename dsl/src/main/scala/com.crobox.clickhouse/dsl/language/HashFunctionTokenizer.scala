package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl._
import com.dongxiguo.fastring.Fastring.Implicits._

trait HashFunctionTokenizer {
  self: ClickhouseTokenizerModule =>
  
  def tokenizeHashFunction(col: HashFunction): String = col match {
    case HalfMD5(col: StringColMagnet) => fast""
    case MD5(col: StringColMagnet) => fast""
    case SipHash64(col: StringColMagnet) => fast""
    case SipHash128(col: StringColMagnet) => fast""
    case CityHash64(col1: AnyTableColumn, coln: Seq[AnyTableColumn]) => fast""
    case IntHash32(col: NumericCol) => fast""
    case IntHash64(col: NumericCol) => fast""
    case SHA1(col: AnyTableColumn) => fast""
    case SHA224(col: AnyTableColumn) => fast""
    case SHA256(col: AnyTableColumn) => fast""
    case URLHash(col: AnyTableColumn, depth: NumericCol) => fast""
  }

}
