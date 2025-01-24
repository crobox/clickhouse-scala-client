package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{Column, ExpressionColumn}

trait IPFunctions { self: Magnets =>
  abstract class IPFunction[O](col: Column) extends ExpressionColumn[O](col)

  case class IPv4NumToString(col: NumericCol[_])       extends IPFunction[String](col.column)
  case class IPv4StringToNum(col: StringColMagnet[_])  extends IPFunction[Long](col.column)
  case class IPv4NumToStringClassC(col: NumericCol[_]) extends IPFunction[String](col.column)
  case class IPv6NumToString(col: StringColMagnet[_])  extends IPFunction[String](col.column)
  case class IPv6StringToNum(col: StringColMagnet[_])  extends IPFunction[Long](col.column)

  def iPv4NumToString(col: NumericCol[_])       = IPv4NumToString(col)
  def iPv4StringToNum(col: StringColMagnet[_])  = IPv4StringToNum(col)
  def iPv4NumToStringClassC(col: NumericCol[_]) = IPv4NumToStringClassC(col)
  def iPv6NumToString(col: StringColMagnet[_])  = IPv6NumToString(col)
  def iPv6StringToNum(col: StringColMagnet[_])  = IPv6StringToNum(col)
  /*
IPv4NumToString(num)
IPv4StringToNum(s)
IPv4NumToStringClassC(num)
IPv6NumToString(x)
IPv6StringToNum(s)
   */
}
