package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{ExpressionColumn, TableColumn}
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn

trait HashFunctions { self: Magnets =>
  abstract class HashFunction(col: AnyTableColumn) extends ExpressionColumn[String](col)

  case class HalfMD5(col: StringColMagnet)                           extends HashFunction(col.column)
  case class MD5(col: StringColMagnet)                               extends HashFunction(col.column)
  case class SipHash64(col: StringColMagnet)                         extends HashFunction(col.column)
  case class SipHash128(col: StringColMagnet)                        extends HashFunction(col.column)
  case class CityHash64(col1: AnyTableColumn, coln: AnyTableColumn*) extends HashFunction(col1)
  case class IntHash32(col: NumericCol)                              extends HashFunction(col.column)
  case class IntHash64(col: NumericCol)                              extends HashFunction(col.column)
  case class SHA1(col: AnyTableColumn)                               extends HashFunction(col)
  case class SHA224(col: AnyTableColumn)                             extends HashFunction(col)
  case class SHA256(col: AnyTableColumn)                             extends HashFunction(col)
  case class URLHash(col: AnyTableColumn, depth: NumericCol)         extends HashFunction(col)

  def halfMD5(col: StringColMagnet)                           = HalfMD5(col)
  def mD5(col: StringColMagnet)                               = MD5(col)
  def sipHash64(col: StringColMagnet)                         = SipHash64(col)
  def sipHash128(col: StringColMagnet)                        = SipHash128(col)
  def cityHash64(col1: AnyTableColumn, coln: AnyTableColumn*) = CityHash64(col1, coln: _*)
  def intHash32(col: NumericCol)                              = IntHash32(col)
  def intHash64(col: NumericCol)                              = IntHash64(col)
  def sHA1(col: AnyTableColumn)                               = SHA1(col)
  def sHA224(col: AnyTableColumn)                             = SHA224(col)
  def sHA256(col: AnyTableColumn)                             = SHA256(col)
  def uRLHash(col: AnyTableColumn, depth: Int)                = URLHash(col, depth)

  /*
halfMD5
MD5
sipHash64
sipHash128
cityHash64
intHash32
intHash64
SHA1
SHA224
SHA256
URLHash(url[, N])
 */
}
