package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.ExpressionColumn

trait BitFunctions { self: Magnets =>
  abstract class BitFunction(a: NumericCol) extends ExpressionColumn[Long](a.column)

  case class BitAnd(a: NumericCol, b: NumericCol)        extends BitFunction(a)
  case class BitOr(a: NumericCol, b: NumericCol)         extends BitFunction(a)
  case class BitXor(a: NumericCol, b: NumericCol)        extends BitFunction(a)
  case class BitNot(a: NumericCol)                       extends BitFunction(a)
  case class BitShiftLeft(a: NumericCol, b: NumericCol)  extends BitFunction(a)
  case class BitShiftRight(a: NumericCol, b: NumericCol) extends BitFunction(a)

  def bitAnd(a: NumericCol, b: NumericCol)        = BitAnd(a: NumericCol, b: NumericCol)
  def bitOr(a: NumericCol, b: NumericCol)         = BitOr(a: NumericCol, b: NumericCol)
  def bitXor(a: NumericCol, b: NumericCol)        = BitXor(a: NumericCol, b: NumericCol)
  def bitNot(a: NumericCol)                       = BitNot(a: NumericCol)
  def bitShiftLeft(a: NumericCol, b: NumericCol)  = BitShiftLeft(a: NumericCol, b: NumericCol)
  def bitShiftRight(a: NumericCol, b: NumericCol) = BitShiftRight(a: NumericCol, b: NumericCol)

  /*
  bitAnd(a, b)
  bitOr(a, b)
  bitXor(a, b)
  bitNot(a)
  bitShiftLeft(a, b)
  bitShiftRight(a, b)
 */
}
