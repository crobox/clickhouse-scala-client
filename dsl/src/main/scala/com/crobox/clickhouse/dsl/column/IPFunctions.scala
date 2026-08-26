package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{Column, ExpressionColumn, FixedStringBytes}

trait IPFunctions { self: Magnets =>
  abstract class IPFunction[O](col: Column) extends ExpressionColumn[O](col)

  case class IPv4NumToString(col: NumericCol[_])       extends IPFunction[String](col.column)
  case class IPv4StringToNum(col: StringColMagnet[_])  extends IPFunction[Long](col.column)
  case class IPv4NumToStringClassC(col: NumericCol[_]) extends IPFunction[String](col.column)

  // IPv6StringToNum returns FixedString(16), and IPv6NumToString accepts nothing else -- passing it a String
  // fails at the server with ILLEGAL_TYPE_OF_ARGUMENT.
  case class IPv6NumToString(col: FixedStringColMagnet[_]) extends IPFunction[String](col.column)
  case class IPv6StringToNum(col: StringColMagnet[_])      extends IPFunction[FixedStringBytes](col.column)

  def iPv4NumToString(col: NumericCol[_])           = IPv4NumToString(col)
  def iPv4StringToNum(col: StringColMagnet[_])      = IPv4StringToNum(col)
  def iPv4NumToStringClassC(col: NumericCol[_])     = IPv4NumToStringClassC(col)
  def iPv6NumToString(col: FixedStringColMagnet[_]) = IPv6NumToString(col)
  def iPv6StringToNum(col: StringColMagnet[_])      = IPv6StringToNum(col)
}
