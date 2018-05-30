package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.ExpressionColumn
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn

trait IPFunctions { self: Magnets =>
  abstract class IPFunction[O](col: AnyTableColumn) extends ExpressionColumn[O](col)

  case class IPv4NumToString(col: NumericCol)       extends IPFunction[String](col.column)
  case class IPv4StringToNum(col: StringColMagnet)  extends IPFunction[Long](col.column)
  case class IPv4NumToStringClassC(col: NumericCol) extends IPFunction[String](col.column)
  case class IPv6NumToString(col: NumericCol)       extends IPFunction[String](col.column)
  case class IPv6StringToNum(col: StringColMagnet)  extends IPFunction[Long](col.column)

  def iPv4NumToString(col: NumericCol)       = IPv4NumToString(col)
  def iPv4StringToNum(col: StringColMagnet)  = IPv4StringToNum(col)
  def iPv4NumToStringClassC(col: NumericCol) = IPv4NumToStringClassC(col)
  def iPv6NumToString(col: NumericCol)       = IPv6NumToString(col)
  def iPv6StringToNum(col: StringColMagnet)  = IPv6StringToNum(col)
  /*
IPv4NumToString(num)
IPv4StringToNum(s)
IPv4NumToStringClassC(num)
IPv6NumToString(x)
IPv6StringToNum(s)
 */
}
