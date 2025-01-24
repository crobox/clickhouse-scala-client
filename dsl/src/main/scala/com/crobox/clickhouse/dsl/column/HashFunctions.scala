package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{Column, ExpressionColumn}

trait HashFunctions { self: Magnets =>
  abstract class HashFunction(col: Column) extends ExpressionColumn[String](col)

  case class HalfMD5(col: StringColMagnet[_])                                  extends HashFunction(col.column)
  case class MD5(col: StringColMagnet[_])                                      extends HashFunction(col.column)
  case class SipHash64(col: StringColMagnet[_])                                extends HashFunction(col.column)
  case class SipHash128(col: StringColMagnet[_])                               extends HashFunction(col.column)
  case class CityHash64(col1: ConstOrColMagnet[_], coln: ConstOrColMagnet[_]*) extends HashFunction(col1.column)
  case class IntHash32(col: NumericCol[_])                                     extends HashFunction(col.column)
  case class IntHash64(col: NumericCol[_])                                     extends HashFunction(col.column)
  case class SHA1(col: ConstOrColMagnet[_])                                    extends HashFunction(col.column)
  case class SHA224(col: ConstOrColMagnet[_])                                  extends HashFunction(col.column)
  case class SHA256(col: ConstOrColMagnet[_])                                  extends HashFunction(col.column)
  case class URLHash(col: ConstOrColMagnet[_], depth: NumericCol[_])           extends HashFunction(col.column)

  def halfMD5(col: StringColMagnet[_])                                  = HalfMD5(col)
  def mD5(col: StringColMagnet[_])                                      = MD5(col)
  def sipHash64(col: StringColMagnet[_])                                = SipHash64(col)
  def sipHash128(col: StringColMagnet[_])                               = SipHash128(col)
  def cityHash64(col1: ConstOrColMagnet[_], coln: ConstOrColMagnet[_]*) = CityHash64(col1, coln: _*)
  def intHash32(col: NumericCol[_])                                     = IntHash32(col)
  def intHash64(col: NumericCol[_])                                     = IntHash64(col)
  def sHA1(col: ConstOrColMagnet[_])                                    = SHA1(col)
  def sHA224(col: ConstOrColMagnet[_])                                  = SHA224(col)
  def sHA256(col: ConstOrColMagnet[_])                                  = SHA256(col)
  def uRLHash(col: ConstOrColMagnet[_], depth: NumericCol[_])           = URLHash(col, depth)
}
