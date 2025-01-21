package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.ExpressionColumn

trait BitFunctions { self: Magnets =>
  abstract class BitFunction(a: NumericCol[_]) extends ExpressionColumn[Long](a.column)

  case class BitAnd(a: NumericCol[_], b: NumericCol[_])        extends BitFunction(a)
  case class BitOr(a: NumericCol[_], b: NumericCol[_])         extends BitFunction(a)
  case class BitXor(a: NumericCol[_], b: NumericCol[_])        extends BitFunction(a)
  case class BitNot(a: NumericCol[_])                          extends BitFunction(a)
  case class BitShiftLeft(a: NumericCol[_], b: NumericCol[_])  extends BitFunction(a)
  case class BitShiftRight(a: NumericCol[_], b: NumericCol[_]) extends BitFunction(a)

  def bitAnd(a: NumericCol[_], b: NumericCol[_]): BitAnd             = BitAnd(a: NumericCol[_], b: NumericCol[_])
  def bitOr(a: NumericCol[_], b: NumericCol[_]): BitOr               = BitOr(a: NumericCol[_], b: NumericCol[_])
  def bitXor(a: NumericCol[_], b: NumericCol[_]): BitXor             = BitXor(a: NumericCol[_], b: NumericCol[_])
  def bitNot(a: NumericCol[_]): BitNot                               = BitNot(a: NumericCol[_])
  def bitShiftLeft(a: NumericCol[_], b: NumericCol[_]): BitShiftLeft = BitShiftLeft(a: NumericCol[_], b: NumericCol[_])
  def bitShiftRight(a: NumericCol[_], b: NumericCol[_]): BitShiftRight =
    BitShiftRight(a: NumericCol[_], b: NumericCol[_])
}
